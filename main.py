import asyncio
import concurrent.futures
import logging
from datetime import datetime

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from config import (
    BOT_TOKEN, ADMIN_ID, DATABASE_FILE,
    GARENA_BASE_URL, GARENA_APP_ID, GARENA_USER_AGENT,
    TIMEZONE, START_TIME_HOUR, START_TIME_MINUTE, INTERVAL_SECONDS,
    MAX_EMAILS, MAX_DAILY_SENDS_PER_EMAIL,
    REQUIRED_REFERRALS_PER_BURN, MAX_BIND_SEARCHES_PER_DAY,
    LOG_LEVEL, LOG_FORMAT,
)
from api_clients import (
    DatabaseClient, GarenaClient,
    is_valid_token, truncate_token, TZ,
)
from services import BindService, OtpService, ReferralService
import keyboards as kb

logging.basicConfig(format=LOG_FORMAT, level=getattr(logging, LOG_LEVEL, logging.INFO))
logger = logging.getLogger(__name__)

RUNNING_TASKS = {}
USER_ACCOUNTS_CACHE = {}

# --- شاشة OTP الموحّدة ---
BURN_STATUS = {}            # user_id -> OTP screen message_id
BURN_UPDATER_TASKS = {}     # user_id -> asyncio.Task (background updater)
SCREEN_UPDATE_INTERVAL = 5  # ثواني بين كل تحديث للشاشة


def is_running(user_id: int) -> bool:
    task = RUNNING_TASKS.get(user_id)
    return task is not None and not task.done()


def get_services(context: ContextTypes.DEFAULT_TYPE):
    return (
        context.bot_data["bind_service"],
        context.bot_data["otp_service"],
        context.bot_data["referral_service"],
    )


# ============= شاشة OTP =============

def build_otp_screen_text(user_id, context, extra=""):
    otp_service = context.bot_data["otp_service"]
    emails = otp_service.load_emails(user_id)
    counts = otp_service.get_all_counts(user_id)
    total = sum(counts.get(e, 0) for e in emails)
    ts = datetime.now(TZ).strftime("%H:%M:%S")

    lines = ["📊 OTP Monitor", f"Total sent: {total}", f"Last update: {ts}"]
    if extra:
        lines.append(extra)
    lines.append("")
    if not emails:
        lines.append("(no emails)")
    else:
        for e in emails:
            lines.append(f"• {e}: {counts.get(e, 0)}")

    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:4000] + "\n... (truncated)"
    return text


async def push_otp_screen(user_id, context, extra="", force_new=False, allow_new=True):
    """يرسل مرة أولى ثم يعدّل نفس الرسالة، بدون إرسال رسائل جديدة."""
    text = build_otp_screen_text(user_id, context, extra)
    msg_id = None if force_new else BURN_STATUS.get(user_id)

    if msg_id:
        try:
            await context.bot.edit_message_text(
                chat_id=user_id, message_id=msg_id, text=text,
            )
            return
        except Exception as e:
            if "message is not modified" in str(e).lower():
                return
            logger.warning(f"edit screen ({user_id}): {e}")
            if not allow_new:
                return
            BURN_STATUS.pop(user_id, None)

    try:
        msg = await context.bot.send_message(chat_id=user_id, text=text)
        BURN_STATUS[user_id] = msg.message_id
    except Exception as e:
        logger.error(f"send screen ({user_id}): {e}")


async def otp_screen_updater(user_id, context):
    """Task مستقل يحدّث الشاشة كل 5 ثواني."""
    while True:
        await asyncio.sleep(SCREEN_UPDATE_INTERVAL)
        try:
            await push_otp_screen(user_id, context, allow_new=False)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"updater ({user_id}): {e}")


# ============= إرسال OTP واحد (async) =============

async def send_one_async(email, user_id, otp_service):
    """يرسل OTP ويحدّث العداد — يُستدعى بالتوازي مع إيميلات أخرى."""
    try:
        await asyncio.to_thread(otp_service.send_one, email)
    except Exception as e:
        logger.error(f"send({email}): {e}")
        return False
    try:
        current = await asyncio.to_thread(otp_service.get_count, user_id, email)
        await asyncio.to_thread(otp_service.set_count, user_id, email, current + 1)
    except Exception as e:
        logger.error(f"count({email}): {e}")
    return True


# ============= حلقة Burn =============

async def burn_loop(user_id, context):
    otp_service = context.bot_data["otp_service"]
    db = context.bot_data["db"]

    BURN_STATUS.pop(user_id, None)
    await push_otp_screen(user_id, context, extra="🚀 Starting...", force_new=True)

    updater = asyncio.create_task(otp_screen_updater(user_id, context))
    BURN_UPDATER_TASKS[user_id] = updater

    last_reset_day = db.get_last_reset(user_id)

    try:
        while True:
            emails = otp_service.load_emails(user_id)
            if not emails:
                try:
                    await push_otp_screen(
                        user_id, context,
                        extra="⚠️ No emails. Stopped.",
                        allow_new=False,
                    )
                except Exception:
                    pass
                return

            now = datetime.now(TZ)
            today = now.strftime("%Y-%m-%d")

            if (
                (now.hour > START_TIME_HOUR or
                 (now.hour == START_TIME_HOUR and now.minute >= START_TIME_MINUTE))
                and last_reset_day != today
            ):
                otp_service.reset_counts(user_id)
                db.set_last_reset(user_id, today)
                last_reset_day = today

            active = [
                e for e in emails
                if otp_service.get_count(user_id, e) < MAX_DAILY_SENDS_PER_EMAIL
            ]

            if not active:
                # الكل وصل الحد → انتظر (لكن يبقى قابل للإيقاف)
                for _ in range(12):
                    await asyncio.sleep(5)
                continue

            # ====== الإرسال بالتوازي — أقصى سرعة ممكنة ======
            tasks = [
                asyncio.create_task(send_one_async(e, user_id, otp_service))
                for e in active
            ]
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
            except asyncio.CancelledError:
                for t in tasks:
                    t.cancel()
                raise

            if INTERVAL_SECONDS > 0:
                await asyncio.sleep(INTERVAL_SECONDS)

    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.error(f"burn_loop ({user_id}): {e}")
    finally:
        updater.cancel()
        try:
            await updater
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.warning(f"updater finish ({user_id}): {e}")
        BURN_UPDATER_TASKS.pop(user_id, None)


# ============= أوامر =============

async def show_main_menu(target, user_id, context):
    otp_service = context.bot_data["otp_service"]
    emails = otp_service.load_emails(user_id)
    running = is_running(user_id)
    markup = kb.main_menu(user_id, ADMIN_ID, running, len(emails), MAX_EMAILS)
    text = "Main Menu:"
    if hasattr(target, "edit_message_text"):
        await target.edit_message_text(text, reply_markup=markup)
    else:
        await target.reply_text(text, reply_markup=markup)


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    referral_service = context.bot_data["referral_service"]

    if context.args and context.args[0].isdigit():
        referrer = int(context.args[0])
        result = referral_service.register(user_id, referrer)
        if result["success"] and result["credit"]:
            try:
                await context.bot.send_message(
                    chat_id=referrer,
                    text=f"You earned 1 credit! ({REQUIRED_REFERRALS_PER_BURN} referrals reached)",
                )
            except Exception:
                pass

    admin_tag = " [Admin]" if user_id == ADMIN_ID else ""
    await update.message.reply_text(
        f"Free Fire Recovery Bot{admin_tag}\n\nChoose an option:",
        reply_markup=kb.main_menu(user_id, ADMIN_ID, False, 0, MAX_EMAILS),
    )


async def bind_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /bind <access_token>")
        return
    token = context.args[0].strip().lower()
    user_id = update.effective_user.id
    if not is_valid_token(token):
        await update.message.reply_text("Invalid token. Must be 64 hex chars.")
        return
    wait = await update.message.reply_text("Searching...")
    bind_service = context.bot_data["bind_service"]
    result = await bind_service.search(token, user_id)
    if not result["success"]:
        errors = {
            "daily_limit": "Daily limit reached.",
            "invalid_token": "Invalid token or no data.",
            "no_binding": "Account has no email binding.",
        }
        await wait.edit_text(errors.get(result["error"], "Unknown error."))
        return
    await wait.delete()
    await update.message.reply_text(
        f"Account found\n\nEmail: {result['email']}\n"
        f"Mobile: {result['mobile'] or '-'}\nToken: {truncate_token(token)}",
        reply_markup=kb.control_menu(token),
    )


async def addcredits_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_ID:
        return
    if len(context.args) != 2:
        await update.message.reply_text("Usage: /addcredits <user_id> <amount>")
        return
    try:
        target = int(context.args[0])
        amount = int(context.args[1])
    except ValueError:
        await update.message.reply_text("Invalid numbers.")
        return
    referral_service = context.bot_data["referral_service"]
    total = referral_service.add_credits(target, amount)
    await update.message.reply_text(f"Added {amount} credits to {target}. Total: {total}")
    try:
        await context.bot.send_message(
            chat_id=target, text=f"Admin granted {amount} credit(s).",
        )
    except Exception:
        pass


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    data = query.data

    bind_service, otp_service, referral_service = get_services(context)

    if data == "btn_main_menu":
        await show_main_menu(query, user_id, context)
        return

    # ===== تشغيل Burn =====
    if data == "btn_start_burn":
        if is_running(user_id):
            await query.answer("Already running!", show_alert=True)
            return
        emails = otp_service.load_emails(user_id)
        if not emails:
            await query.edit_message_text(
                "Add an email first.",
                reply_markup=kb.main_menu(user_id, ADMIN_ID, False, 0, MAX_EMAILS),
            )
            return

        if user_id != ADMIN_ID:
            if not referral_service.use_credit(user_id):
                stats = referral_service.stats(user_id)
                await query.edit_message_text(
                    f"Need a credit.\n\nYour referrals: {stats['referrals']}\n"
                    f"Progress: {stats['progress']}/{REQUIRED_REFERRALS_PER_BURN}\n\n"
                    f"Get {REQUIRED_REFERRALS_PER_BURN} referrals for 1 credit.",
                    reply_markup=kb.main_menu(user_id, ADMIN_ID, False, len(emails), MAX_EMAILS),
                )
                return

        task = asyncio.create_task(burn_loop(user_id, context))
        RUNNING_TASKS[user_id] = task

        await query.edit_message_text(
            f"Burn started - {len(emails)} email(s)",
            reply_markup=kb.main_menu(user_id, ADMIN_ID, True, len(emails), MAX_EMAILS),
        )
        return

    # ===== إيقاف Burn =====
    if data == "btn_stop_burn":
        task = RUNNING_TASKS.pop(user_id, None)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.warning(f"stop burn: {e}")

        # تحويل شاشة OTP إلى "Stopped"
        status_id = BURN_STATUS.pop(user_id, None)
        if status_id:
            try:
                await context.bot.edit_message_text(
                    chat_id=user_id, message_id=status_id, text="⏹ Stopped.",
                )
            except Exception as e:
                logger.warning(f"stop screen: {e}")

        emails = otp_service.load_emails(user_id)
        await query.edit_message_text(
            "Stopped.",
            reply_markup=kb.main_menu(user_id, ADMIN_ID, False, len(emails), MAX_EMAILS),
        )
        return

    if data == "btn_add_account":
        context.user_data["state"] = "token"
        await query.edit_message_text("Send the access_token (64 hex chars).")
        return

    if data == "btn_my_accounts":
        accounts = bind_service.get_user_accounts(user_id)
        USER_ACCOUNTS_CACHE[user_id] = accounts
        if not accounts:
            await query.edit_message_text(
                "No saved accounts.",
                reply_markup=kb.main_menu(user_id, ADMIN_ID, is_running(user_id), 0, MAX_EMAILS),
            )
            return
        await query.edit_message_text(
            f"Your accounts ({len(accounts)}):", reply_markup=kb.accounts_menu(accounts),
        )
        return

    if data == "btn_control_menu":
        accounts = bind_service.get_user_accounts(user_id)
        USER_ACCOUNTS_CACHE[user_id] = accounts
        if not accounts:
            await query.edit_message_text(
                "No accounts.",
                reply_markup=kb.main_menu(user_id, ADMIN_ID, False, 0, MAX_EMAILS),
            )
            return
        await query.edit_message_text("Choose account:", reply_markup=kb.accounts_menu(accounts))
        return

    if data.startswith("acc_select_"):
        idx = int(data.split("_")[-1])
        accounts = USER_ACCOUNTS_CACHE.get(user_id, [])
        if idx >= len(accounts):
            await query.answer("Not found", show_alert=True)
            return
        acc = accounts[idx]
        token = acc["token"]
        text = (
            f"Selected account\n\nEmail: {acc.get('email', '?')}\n"
            f"Mobile: {acc.get('mobile') or '-'}\nToken: {truncate_token(token)}"
        )
        await query.edit_message_text(text, reply_markup=kb.control_menu(token))
        return

    if data.startswith("ctrl_info_"):
        token_short = data.replace("ctrl_info_", "")
        accounts = USER_ACCOUNTS_CACHE.get(user_id, [])
        acc = next((a for a in accounts if a["token"].startswith(token_short)), None)
        if not acc:
            await query.answer("Not found", show_alert=True)
            return
        text = (
            f"Full Info\n\nEmail: {acc.get('email', '?')}\n"
            f"Mobile: {acc.get('mobile') or '-'}\n"
            f"Token: {acc.get('token', '?')}\nSaved: {acc.get('timestamp', '-')}"
        )
        await query.edit_message_text(text, reply_markup=kb.control_menu(acc["token"]))
        return

    if data.startswith("ctrl_otp_"):
        token_short = data.replace("ctrl_otp_", "")
        accounts = USER_ACCOUNTS_CACHE.get(user_id, [])
        acc = next((a for a in accounts if a["token"].startswith(token_short)), None)
        if not acc:
            await query.answer("Not found", show_alert=True)
            return
        email = acc.get("email")
        await query.answer("Sending...")
        ok = await send_one_async(email, user_id, otp_service)
        await query.answer("Sent" if ok else "Failed", show_alert=True)
        return

    if data.startswith("ctrl_burn_"):
        token_short = data.replace("ctrl_burn_", "")
        accounts = USER_ACCOUNTS_CACHE.get(user_id, [])
        acc = next((a for a in accounts if a["token"].startswith(token_short)), None)
        if not acc:
            await query.answer("Not found", show_alert=True)
            return
        emails = otp_service.load_emails(user_id)
        if acc["email"] in emails:
            await query.answer("Already in list", show_alert=True)
            return
        if len(emails) >= MAX_EMAILS:
            await query.answer(f"Limit reached ({MAX_EMAILS})", show_alert=True)
            return
        emails.append(acc["email"])
        otp_service.save_emails(user_id, emails)
        await query.answer(f"Added {acc['email']}", show_alert=True)
        return

    if data.startswith("ctrl_del_"):
        token_short = data.replace("ctrl_del_", "")
        accounts = USER_ACCOUNTS_CACHE.get(user_id, [])
        acc = next((a for a in accounts if a["token"].startswith(token_short)), None)
        if not acc:
            await query.answer("Not found", show_alert=True)
            return
        bind_service.remove_account(acc["token"])
        await query.answer("Removed", show_alert=True)
        accounts = bind_service.get_user_accounts(user_id)
        USER_ACCOUNTS_CACHE[user_id] = accounts
        if not accounts:
            await query.edit_message_text(
                "No accounts.",
                reply_markup=kb.main_menu(user_id, ADMIN_ID, False, 0, MAX_EMAILS),
            )
        else:
            await query.edit_message_text(
                f"Your accounts ({len(accounts)}):", reply_markup=kb.accounts_menu(accounts),
            )
        return

    if data == "btn_emails_menu":
        emails = otp_service.load_emails(user_id)
        await query.edit_message_text(
            f"Emails ({len(emails)}/{MAX_EMAILS})", reply_markup=kb.emails_menu(),
        )
        return

    if data == "btn_email_add":
        context.user_data["state"] = "email"
        await query.edit_message_text("Send the new email:")
        return

    if data == "btn_email_view":
        emails = otp_service.load_emails(user_id)
        if not emails:
            await query.edit_message_text("No emails.", reply_markup=kb.emails_menu())
            return
        counts = otp_service.get_all_counts(user_id)
        lines = [
            f"{i+1}. {e} - {counts.get(e, 0)}/{MAX_DAILY_SENDS_PER_EMAIL}"
            for i, e in enumerate(emails)
        ]
        await query.edit_message_text(
            f"Emails ({len(emails)}/{MAX_EMAILS}):\n\n" + "\n".join(lines),
            reply_markup=kb.emails_menu(),
        )
        return

    if data == "btn_email_delete":
        emails = otp_service.load_emails(user_id)
        if not emails:
            await query.edit_message_text("No emails.", reply_markup=kb.emails_menu())
            return
        await query.edit_message_text(
            "Choose email to delete:", reply_markup=kb.delete_email_menu(emails),
        )
        return

    if data.startswith("email_del_"):
        idx = int(data.split("_")[-1])
        removed = otp_service.remove_email(user_id, idx)
        if not removed:
            await query.answer("Not found", show_alert=True)
            return
        emails = otp_service.load_emails(user_id)
        if not emails:
            await query.edit_message_text(
                f"Removed {removed}.\nNo emails left.", reply_markup=kb.emails_menu(),
            )
        else:
            await query.edit_message_text(
                f"Removed {removed}.\nChoose next:", reply_markup=kb.delete_email_menu(emails),
            )
        return

    # ===== إرسال OTP يدوي — نفس الشاشة الموحّدة، إرسال متوازي =====
    if data == "btn_send_otp":
        emails = otp_service.load_emails(user_id)
        if not emails:
            await query.edit_message_text(
                "No emails.",
                reply_markup=kb.main_menu(user_id, ADMIN_ID, is_running(user_id), 0, MAX_EMAILS),
            )
            return

        BURN_STATUS.pop(user_id, None)
        await push_otp_screen(
            user_id, context, extra="📤 Sending OTPs...", force_new=True,
        )

        tasks = [asyncio.create_task(send_one_async(e, user_id, otp_service)) for e in emails]
        await asyncio.gather(*tasks, return_exceptions=True)

        await push_otp_screen(user_id, context, extra="✅ Done")
        return

    if data == "btn_status":
        now = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
        running = is_running(user_id)
        emails = otp_service.load_emails(user_id)
        counts = otp_service.get_all_counts(user_id)
        lines = [f"{e}: {counts.get(e, 0)}/{MAX_DAILY_SENDS_PER_EMAIL}" for e in emails]
        body = "\n".join(lines) if lines else "- none"
        await query.edit_message_text(
            f"Status\n\nRunning: {'Yes' if running else 'No'}\n"
            f"Emails:\n{body}\n\nTime: {now}",
            reply_markup=kb.back_to_main(),
        )
        return

    if data == "btn_referral":
        stats = referral_service.stats(user_id)
        me = await context.bot.get_me()
        link = f"https://t.me/{me.username}?start={user_id}"
        credits = "INF" if user_id == ADMIN_ID else str(stats["credits"])
        await query.edit_message_text(
            f"Referral System\n\nLink: {link}\n\n"
            f"Your referrals: {stats['referrals']}\n"
            f"Progress: {stats['progress']}/{REQUIRED_REFERRALS_PER_BURN}\n"
            f"Credits: {credits}",
            reply_markup=kb.back_to_main(),
        )
        return

    if data == "btn_admin":
        if user_id != ADMIN_ID:
            await query.answer("Access denied", show_alert=True)
            return
        await query.edit_message_text("Admin Panel", reply_markup=kb.admin_menu())
        return

    if data == "admin_add_credits":
        if user_id != ADMIN_ID:
            await query.answer("Access denied", show_alert=True)
            return
        context.user_data["state"] = "admin_credits"
        await query.edit_message_text("Send: <user_id> <amount>")
        return


async def text_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()
    state = context.user_data.get("state")

    _, otp_service, referral_service = get_services(context)

    # ===== إضافة إيميل =====
    if state == "email":
        context.user_data.pop("state", None)
        if "@" not in text or "." not in text:
            await update.message.reply_text("صيغة الإيميل غير صحيحة. أرسل إيميل صالح.")
            return
            
        try:
            ok, reason = otp_service.add_email(user_id, text)
            if not ok:
                msg = "Already exists." if reason == "exists" else f"Limit reached ({MAX_EMAILS})."
                await update.message.reply_text(msg)
                return
            emails = otp_service.load_emails(user_id)
            await update.message.reply_text(
                f"Added {text} ({len(emails)}/{MAX_EMAILS})", reply_markup=kb.emails_menu(),
            )
        except Exception as e:
            logger.error(f"Add email error: {e}")
            await update.message.reply_text("حدث خطأ أثناء الإضافة. حاول مرة أخرى.")
        return

    # ===== إضافة توكن =====
    if state == "token":
        context.user_data.pop("state", None)
        if not is_valid_token(text.lower()):
            await update.message.reply_text("Invalid token. Must be 64 hex chars.")
            return
        wait = await update.message.reply_text("Searching...")
        bind_service = context.bot_data["bind_service"]
        result = await bind_service.search(text.lower(), user_id)
        if not result["success"]:
            errors = {
                "daily_limit": "Daily limit reached.",
                "invalid_token": "Invalid token.",
                "no_binding": "No email binding.",
            }
            await wait.edit_text(errors.get(result["error"], "Unknown error."))
            return
        await wait.delete()
        await update.message.reply_text(
            f"Account found\n\nEmail: {result['email']}\n"
            f"Mobile: {result['mobile'] or '-'}\nToken: {truncate_token(text.lower())}",
            reply_markup=kb.control_menu(text.lower()),
        )
        return

    # ===== إضافة كريدت (أدمن) =====
    if state == "admin_credits":
        if user_id != ADMIN_ID:
            context.user_data.pop("state", None)
            return
        parts = text.split()
        if len(parts) != 2 or not parts[0].isdigit() or not parts[1].lstrip("-").isdigit():
            await update.message.reply_text("Use: <user_id> <amount>")
            return
        context.user_data.pop("state", None)
        target = int(parts[0])
        amount = int(parts[1])
        total = referral_service.add_credits(target, amount)
        await update.message.reply_text(
            f"Added {amount} to {target}. Total: {total}", reply_markup=kb.admin_menu(),
        )
        try:
            await context.bot.send_message(
                chat_id=target, text=f"Admin granted {amount} credit(s).",
            )
        except Exception:
            pass
        return

    await update.message.reply_text(
        "Use the buttons:",
        reply_markup=kb.main_menu(user_id, ADMIN_ID, is_running(user_id), 0, MAX_EMAILS),
    )


def build_application():
    db = DatabaseClient(DATABASE_FILE)
    garena = GarenaClient(GARENA_BASE_URL, GARENA_APP_ID, GARENA_USER_AGENT)

    bind_service = BindService(garena, db, MAX_BIND_SEARCHES_PER_DAY)
    otp_service = OtpService(garena, db, INTERVAL_SECONDS, MAX_DAILY_SENDS_PER_EMAIL, MAX_EMAILS)
    referral_service = ReferralService(db, REQUIRED_REFERRALS_PER_BURN)

    async def post_init(application):
        # Thread pool كبير عشان الإرسال المتوازي يكون فعلاً سريع
        loop = asyncio.get_running_loop()
        loop.set_default_executor(concurrent.futures.ThreadPoolExecutor(max_workers=64))

    app = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()

    app.bot_data["db"] = db
    app.bot_data["garena"] = garena
    app.bot_data["bind_service"] = bind_service
    app.bot_data["otp_service"] = otp_service
    app.bot_data["referral_service"] = referral_service

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("bind", bind_cmd))
    app.add_handler(CommandHandler("addcredits", addcredits_cmd))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message_handler))

    return app


if __name__ == "__main__":
    if not BOT_TOKEN:
        print("BOT_TOKEN is not set.")
        exit(1)
    print("Bot starting...")
    app = build_application()
    app.run_polling()