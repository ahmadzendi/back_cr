import os
import psycopg2
import json
from datetime import datetime
from telegram import InputFile, Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

PGHOST = os.environ.get("PGHOST", "")
PGPORT = os.environ.get("PGPORT", "")
PGUSER = os.environ.get("PGUSER", "")
PGPASSWORD = os.environ.get("PGPASSWORD", "")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "")
TOKEN = os.environ.get("TOKEN", "")

def save_request(data):
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=PGUSER,
        password=PGPASSWORD,
        host=PGHOST,
        port=PGPORT
    )
    c = conn.cursor()
    c.execute("DELETE FROM request")
    c.execute("INSERT INTO request (data, updated_at) VALUES (%s, NOW())", (json.dumps(data),))
    conn.commit()
    c.close()
    conn.close()

async def rank_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) != 4:
        await update.message.reply_text("Format: /rank_all YYYY-MM-DD HH:MM YYYY-MM-DD HH:MM")
        return
    t_awal = context.args[0] + " " + context.args[1]
    t_akhir = context.args[2] + " " + context.args[3]
    save_request({"start": t_awal, "end": t_akhir})
    await update.message.reply_text("Permintaan diterima! Silakan cek website untuk hasilnya.")

async def rank_berdasarkan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) != 5:
        await update.message.reply_text("Format: /rank_berdasarkan <kata> YYYY-MM-DD HH:MM YYYY-MM-DD HH:MM")
        return
    kata = context.args[0].lower()
    t_awal = context.args[1] + " " + context.args[2]
    t_akhir = context.args[3] + " " + context.args[4]
    save_request({"start": t_awal, "end": t_akhir, "kata": kata})
    await update.message.reply_text(f"Permintaan ranking berdasarkan kata '{kata}' diterima! Silakan cek website untuk hasilnya.")

async def reset_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=PGUSER,
            password=PGPASSWORD,
            host=PGHOST,
            port=PGPORT
        )
        c = conn.cursor()
        c.execute("DELETE FROM request")
        conn.commit()
        c.close()
        conn.close()
        await update.message.reply_text("Tampilan website sudah direset. Data chat masih aman.")
    except Exception as e:
        await update.message.reply_text(f"Gagal reset tampilan: {e}")

async def reset_2025(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=PGUSER,
            password=PGPASSWORD,
            host=PGHOST,
            port=PGPORT
        )
        c = conn.cursor()
        c.execute("DELETE FROM chat")
        c.execute("DELETE FROM request")
        conn.commit()
        c.close()
        conn.close()
        await update.message.reply_text("Data chat pada database berhasil direset (dihapus).")
    except Exception as e:
        await update.message.reply_text(f"Gagal reset data chat: {e}")

async def export_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=PGUSER,
            password=PGPASSWORD,
            host=PGHOST,
            port=PGPORT
        )
        c = conn.cursor()
        c.execute("SELECT id, username, content, timestamp, timestamp_wib, level FROM chat ORDER BY timestamp")
        rows = c.fetchall()
        c.close()
        conn.close()
        temp_file = "chat_indodax_export.jsonl"
        with open(temp_file, "w", encoding="utf-8") as f:
            for row in rows:
                chat = {
                    "id": row[0],
                    "username": row[1],
                    "content": row[2],
                    "timestamp": row[3],
                    "timestamp_wib": row[4],
                    "level": row[5]
                }
                f.write(json.dumps(chat, ensure_ascii=False) + "\n")
        with open(temp_file, "rb") as f:
            await update.message.reply_document(document=InputFile(f, filename=temp_file))
        os.remove(temp_file)
    except Exception as e:
        await update.message.reply_text(f"Gagal mengirim file: {e}")

async def export_waktu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) != 4:
        await update.message.reply_text("Format: /export_waktu YYYY-MM-DD HH:MM YYYY-MM-DD HH:MM")
        return
    waktu_awal = context.args[0] + " " + context.args[1]
    waktu_akhir = context.args[2] + " " + context.args[3]
    try:
        t_awal = datetime.strptime(waktu_awal, "%Y-%m-%d %H:%M")
        t_akhir = datetime.strptime(waktu_akhir, "%Y-%m-%d %H:%M")
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=PGUSER,
            password=PGPASSWORD,
            host=PGHOST,
            port=PGPORT
        )
        c = conn.cursor()
        c.execute("SELECT id, username, content, timestamp, timestamp_wib, level FROM chat WHERE timestamp_wib BETWEEN %s AND %s ORDER BY timestamp", (waktu_awal, waktu_akhir))
        rows = c.fetchall()
        c.close()
        conn.close()
        hasil = []
        for row in rows:
            chat = {
                "id": row[0],
                "username": row[1],
                "content": row[2],
                "timestamp": row[3],
                "timestamp_wib": row[4],
                "level": row[5]
            }
            t_chat = datetime.strptime(chat["timestamp_wib"], "%Y-%m-%d %H:%M:%S")
            if t_awal <= t_chat <= t_akhir:
                hasil.append(chat)
        if not hasil:
            await update.message.reply_text("Tidak ada data pada rentang waktu tersebut.")
            return
        temp_file = "chat_indodax_filtered.jsonl"
        with open(temp_file, "w", encoding="utf-8") as f:
            for chat in hasil:
                f.write(json.dumps(chat, ensure_ascii=False) + "\n")
        with open(temp_file, "rb") as f:
            await update.message.reply_document(document=InputFile(f, filename=temp_file))
        os.remove(temp_file)
    except Exception as e:
        await update.message.reply_text(f"Gagal export data: {e}")

async def rank_berdasarkan_username(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 5:
        await update.message.reply_text("Format: /rank_berdasarkan_username <username1> <username2> ... YYYY-MM-DD HH:MM YYYY-MM-DD HH:MM")
        return
    usernames = [u.lower() for u in context.args[:-4]]
    t_awal = context.args[-4] + " " + context.args[-3]
    t_akhir = context.args[-2] + " " + context.args[-1]
    save_request({
        "usernames": usernames,
        "start": t_awal,
        "end": t_akhir,
        "mode": "username"
    })
    await update.message.reply_text("Permintaan ranking berdasarkan username diterima! Silakan cek website untuk hasilnya.")

async def rank_level(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) != 1 or not context.args[0].isdigit():
        await update.message.reply_text("Format: /rank_level <nomor_level>\nContoh: /rank_level 2")
        return
    level = int(context.args[0])
    # Simpan filter ke database (tabel request)
    save_request({"level": level, "mode": "level"})
    await update.message.reply_text(f"Permintaan ranking berdasarkan level {level} diterima! Silakan cek website untuk hasilnya.")

if __name__ == "__main__":
    app_telegram = ApplicationBuilder().token(TOKEN).build()
    app_telegram.add_handler(CommandHandler("rank_all", rank_all))
    app_telegram.add_handler(CommandHandler("rank_berdasarkan", rank_berdasarkan))
    app_telegram.add_handler(CommandHandler("reset_data", reset_data))
    app_telegram.add_handler(CommandHandler("reset_2025", reset_2025))
    app_telegram.add_handler(CommandHandler("export_all", export_all))
    app_telegram.add_handler(CommandHandler("export_waktu", export_waktu))
    app_telegram.add_handler(CommandHandler("rank_berdasarkan_username", rank_berdasarkan_username))
    app_telegram.add_handler(CommandHandler("rank_level", rank_level))
    print("Bot Telegram aktif...")
    app_telegram.run_polling()
