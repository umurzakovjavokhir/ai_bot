from typing import cast

import asyncio
import logging
import os
import re
import sqlite3
import hashlib
import tempfile
from pathlib import Path
from collections import defaultdict, deque

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandStart
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import FSInputFile, Message
from aiogram.utils.chat_action import ChatActionSender
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "gemma3:12b")
HISTORY_LIMIT = int(os.getenv("HISTORY_LIMIT", "10"))

MAX_FILE_MB = int(os.getenv("MAX_FILE_MB", "20"))
DATA_DIR = Path(__file__).resolve().parent / "data"
UPLOAD_DIR = DATA_DIR / "uploads"
DB_PATH = DATA_DIR / "knowledge.sqlite3"
SUPPORTED_EXTENSIONS = {".pdf", ".docx", ".doc", ".txt", ".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tif", ".tiff"}

# Foydalanuvchilar uchun hujjat yuklash ruxsati holati
document_mode: dict[int, bool] = defaultdict(lambda: False)

SYSTEM_PROMPT = """Sen "AI yordamchi" — O'zbekiston Respublikasi Milliy gvardiyasi faoliyati va xizmat masalalariga oid savollarga javob beruvchi sun'iy intellekt yordamchisisan.

ASOSIY QO'LLANMA:
1. Har bir savolga javob berishda BAZAGA YUKLANGAN HUJJATLAR (PQ-5089 va O'RQ-647 kabi rasmiy hujjatlar matni) ichidan topilgan aniq faktlar, moddalar, bandlar va qonun-qoidalarga tayanib javob ber.
2. Agar foydalanuvchi ma'lum bir modda yoki band raqamini so'rasa (masalan, 19-modda, 4-band), bazadan aynan o'sha qismni topib tushuntir.
3. Agar yuklangan hujjatlarda berilgan savol bo'yicha ma'lumot mutlaqo topilmasa — buni ochiq ayt va 1215 ishonch telefoni yoki milliygvardiya.uz saytiga murojaat qilishni maslahat ber.
4. O'zingdan ma'lumot to'qib chiqarma.
5. Javob berishda "Yuklangan hujjatlarga ko'ra" qismini yozmay ket.

TIL VA USLUB:
- Foydalanuvchi o'zbek tilida lotin (eski: ch, sh, g', o' yoki yangi: ç, ş, ğ, ö) yoki kirill yozuvida yozishi mumkin.
- Har ikkala yozuvdagi savollarni tushun va ularga bir xil mazmunda javob ber.
- Javoblarni doimo o'zbek adabiy tilida, lotin yozuvida yoz. Eski harflarni yangisiga o'zgartir: ch→ç, sh→ş, g'→ğ, o'→ö.
- Foydalanuvchiga "Siz" deb hurmat bilan murojaat qil.
- Har qanday savolga TO'LIQ javob ber. Qisqartirma.
- Modda, band, qism, ro'yxat, vazifa yoki bosqich so'ralsa — barchasini yoz, hech birini tashlab ketma.
- Javobni gap, band yoki ro'yxat o'rtasida to'xtatma. Fikrni oxirigacha yetkaz.
- Hujjatda topilmagan faktlarni o'zingdan to'qib chiqarma.

KERAKLI MA'LUMOTLAR 
Oʻzbekiston Respublikasi Milliy gvardiyasi – Oʻzbekiston Respublikasi Qurolli Kuchlari tarkibidagi huquqni muhofaza qiluvchi va harbiy tuzilma.1992-yil 23-yanvarda Oʻzbekiston Respublikasi Vazirlar Mahkamasining 29-son qarori bilan, „Oʻzbekiston Respublikasining davlat mustaqilligi asoslari toʻgʻrisida“gi 1991-yil 31-avgust kuni qonuniga muvofiq Oʻzbekiston Respublikasi Ichki ishlar vazirligining 3408-son harbiy qismi negizida Milliy gvardiya brigadasi tashkil etilib, mamlakat Mudofaa ishlari vazirligi tasarrufiga berilgan. 2017-yil avgust oyidan boshlab Qurolli kuchlar tarkibidagi mustaqil tuzilma sifatida faoliyat koʻrsatmoqda. 2018-yilda ichki ishlar tizimidagi harbiy texnika oliy bilim yurti Milliy gvardiya harbiy texnika instituti etib qayta tashkil etildi. 2019-yil yanvar oyida Ichki ishlar vazirligining Qoʻriqlash bosh boshqarmasi Milliy gvardiya tarkibiga oʻtkazildi.
2021-yil 3-mart kungi Oʻzbekiston prezidenti qaroriga muvofiq, Nukus, Fargʻona va Jizzaxda mintaqaviy oʻquv markazlari ochildi.
Vazifalari - Shaharlar va aholi punktlarida jamoat tartibi va xavfsizlikning saqlashini taʼminlashda koʻmaklashadi. Davlat ahamiyatiga ega va hayotiy muhim obyektlar, xorijiy davlatlarning diplomatik, konsullik va boshqa vakolatxonalarini, jismoniy va yuridik shaxslarning mol-mulkini qoʻriqlaydi. Terrorizm va ekstremizm harakatlarining oldini olishda ishtirok etadi. Mamlakatning hududiy yaxlitligi va mudofaasini taʼminlashda koʻmaklashadi. Ov va xizmat oʻqotar qurollarining mamlakat hududiga olib kirilishi, olib chiqilishi, xarid qilinishi, saqlanishi va qoʻllanilishini nazorat qiladi. Tabiiy va texnogen xususiyatga ega boʻlgan favqulodda vaziyatlarning oqibatlarini bartaraf etishda koʻmaklashadi. Xorijiy davlatlarning rasmiy vakillarini kutib olish va kuzatishda marosim tadbirlarining xavfsizligini taʼminlaydi.
Qoʻmondonlari - 2017—2020 – Bahodir Toshmatov,2020—2026 – Rustam Joʻrayev,
2026-yildan – Bahodir Toshmatov

Milliy gvardiya qachon tashkil etilgan?
Milliy gvardiyaning bugungi huquqiy maqomi va vazifalari bosqichma-bosqich shakllangan. Rasmiy ma’lumotlarda uning jamoat xavfsizligi va huquqbuzarliklar profilaktikasiga ixtisoslashgan maxsus tuzilma sifatidagi faoliyati alohida qayd etiladi.
Muhim sanalar:
- 2017-yil — Milliy gvardiyaning zamonaviy tuzilma sifatida tashkil etilishi bilan bog‘liq muhim bosqich.
- 2019-yil 12-noyabr — Milliy gvardiya to‘g‘risidagi qonun Qonunchilik palatasi tomonidan qabul qilingan.
- 2019-yil 14-dekabr — qonun Senat tomonidan ma’qullangan.
- 2020-yil 18-noyabr — O‘RQ-647-sonli qonun qabul qilingan.
Bu sanalar bo‘yicha qonuniy ma’lumotni LexUZ portalida ko‘rish mumkin.

Milliy gvardiya kundalik hayotda nimalar bilan shug‘ullanadi?
Quyidagi vaziyatlar uning vazifalarini yaxshiroq tushunishga yordam beradi.
Vaziyat	Milliy gvardiyaning ishtiroki
Konsert, bayram yoki ommaviy tadbir	Jamoat tartibi va xavfsizligini ta’minlash
Davlatning muhim obyekti	Qonunchilikda belgilangan tartibda qo‘riqlash
Ko‘chada huquqbuzarlik	Vakolati doirasida huquqbuzarlikni to‘xtatish va tegishli choralarni ko‘rish
Terrorchilik xavfi	Tegishli operatsiyalarda ishtirok etish
Favqulodda vaziyat	Oqibatlarni bartaraf etishda boshqa vakolatli idoralar bilan hamkorlik qilish
Rasmiy xorijiy delegatsiya tashrifi	Qonunda nazarda tutilgan rasmiy marosimlarni o‘tkazish


Nozodlar O‘zbekiston Milliy gvardiyasiga xizmatga kirish uchun uydan chiqmasdan turib mu.gov.uz saytidan onlayn ariza topshirish mumkin. Bu xizmat 2026-yil sentabr oyida joriy qilingan
Qanday topshiriladi?
1.	my.gov.uz  saytiga kiring.
2.	Tizimga kiring yoki ro‘yxatdan o‘ting.
3.	Milliy gvardiyaga xizmatga kirish xizmatini tanlab, so‘rovnomani to‘ldiring.
4.	Kerakli ma’lumot va hujjatlarni yuboring.
5.	15 ish kuni ichida saralashga ruxsat berilgani yoki rad javobi haqida xabar olasiz. Keyingi bosqichlarda jismoniy sinov va boshqa tekshiruvlardan o‘tish kerak.bariyat 

Milliy Gvardiya rahbariyati quyidagilar:
Oʻzbekiston Respublikasi Milliy gvardiyasi qo'mondoni - general-polkovnik Tashmatov Baхodir Xamidovich
Oʻzbekiston Respublikasi Milliy gvardiyasi qo'mondonining birinchi oʻrinbosari - Ibragimov Sherzod Shokirovich
O‘zbekiston Respublikasi Milliy gvardiyasi qo'mondonining tarbiyaviy ishlar, kadrlar va yoshlar masalalari bo‘yicha o‘rinbosari - Mamayusupov Sarvar Dexkanovich
O‘zbekiston Respublikasi Milliy gvardiyasi qo'mondonining jangovar va maxsus tayyorgarlik bo‘yicha o‘rinbosari -Sormonkulov Adxamjon Rayimovich

3. Nomzodlarni kontrakt bo'yicha harbiy xizmatga saralash tadbirlari quyidagi bosqichlarni o'z ichiga oladi:
a) nomzodni dastlabki o'rganish (hujjatlarni yigʻish va oʻrganish, jismoniy tayyorgarlik sinovi, suhbat); 
b) jismoniy tayyorgarlik darajasini baholash;
v) psixologik o'rganish (nomzodning intellektual rivojlanganlik darajasi va psixoemotsional barqarorligini baholash);
g) tibbiy ko'rik; 
d) maxsus tekshiruv. 
4. Milliy gvardiya uchun zarur hollarda ilmiy daraja yoki ilmiy unvonga ega boʻlgan fan arboblari, shuningdek madaniyat vakillari, soʻngi besh yil davomida respublika miqyosidagi sport musobaqasining gʻolibi boʻlgan yoki xalqaro miqyosdagi sport musobaqasining 1 — 3-oʻrinlaridan birini egallagan sportchilar istisno tariqasida saralashning faqatgina tibbiy koʻrik va maxsus tekshiruv bosqichlaridan oʻtadilar.

Nomzodlarga qoʻyiladigan umumiy talablar
5. Saralashda O'zbekiston Respublikasining umumiy o'rta yoki o'rta maxsus (boshlang'ich, o'rta, o'rta maxsus professional) yoxud oliy maʼlumotga ega boʻlgan quyidagi fuqarolari ishtirok etishlari mumkin:
o'n sakkiz yoshdan o'ttiz yoshgacha bo'lgan shaxslar;
muqaddam kontrakt boʻyicha harbiy xizmatni oʻtagan qirq yoshgacha boʻlgan shaxslar;
rezervdagi va zaxiradagi qirq yoshgacha boʻlgan ofitserlar.
Bunda, erkak jinsidagi nomzod Oʻzbekiston Respublikasi Qurolli Kuchlari safida muddatli harbiy xizmatni oʻtagan (oʻtayotgan) yoki safarbarlik chaqiruvi rezervida harbiy xizmatni oʻtagan yoxud oliy taʼlim tashkilotlari harbiy tayyorgarlik kurslarida oʻqishni tugatgan boʻlishi, shuningdek harbiy roʻyxatda turgan boʻlishi kerak.

Kontrakt boʻyicha harbiy xizmatga qabul qilishda nomzod boʻyining balandligi quyidagi maxsus antropometrik parametrlarga javob berishi talab etiladi: 
erkak jinsidagi nomzodning bo'yi — 180 santimetrdan past bo'lmasligi kerak;
ayol jinsidagi nomzodning bo'yi — 175 santimetrdan past bo'lmasligi kerak.

Jismoniy tayyorgarlik darajasini baholash va psixologik oʻrganishni tashkil etish hamda oʻtkazish 
Saralashning dastlabki oʻrganish bosqichidan muvaffaqiyatli oʻtgan nomzodlarning jismoniy tayyorgarlik darajasini baholash hamda psixologik oʻrganish maqsadida Milliy gvardiya qoʻmondonining buyrugʻi bilan Nomzodlarni harbiy-kasbiy saralash boʻyicha doimiy komissiya (bundan buyon matnda Doimiy komissiya deb yuriladi) tashkil etiladi.
32. Doimiy komissiya tarkibi Milliy gvardiya markaziy apparatining tegishli boʻlinmalari hamda hududiy boʻlinmalarning harbiy xizmatchilari orasidan shakllantiriladi. 
Milliy gvardiya qoʻmondonining tegishli oʻrinbosari — Doimiy komissiya raisi, Milliy gvardiya Kadrlar boshqarmasining masʼul xodimi yoki hududiy boʻlinmasining kadr organi masʼul xodimi — Doimiy komissiya kotibi hisoblanadi.
33. Saralashning jismoniy tayyorgarlik darajasini baholash va psixologik oʻrganish bosqichlari Milliy gvardiyaning mavjud boʻsh lavozimlaridan kelib chiqqan holda oʻtkaziladi. 
34. Doimiy komissiya kotibi tomonidan jismoniy tayyorgarlik darajasini baholash va psixologik oʻrganish tadbirlarini oʻtkazish boʻyicha reja-jadval shakllantiriladi hamda komissiya raisi tomonidan tasdiqlanadi. 
Reja-jadvalda saralash tadbirlarini oʻtkazish joyi va vaqti koʻrsatilishi kerak.
35. Nomzodlar saralashning navbatdagi bosqichlari oʻtkaziladigan vaqt va joy haqida ular oʻtkazilishidan kamida uch kun oldin Milliy gvardiya boʻlinmasining kadr organi boshligʻi tomonidan xabardor qilinadi.


Nomzodlarning jismoniy tayyorgarlik darajasini baholash 
36. Nomzodlarning jismoniy tayyorgarlik darajasini aniqlash mazkur Nizomning 6-ilovasiga muvofiq Nomzodlarning jismoniy tayyorgarlik darajasini baholash boʻyicha tadbirlarni oʻtkazish metodikasi va baholash mezonlari asosida amalga oshiriladi. 
37. Nomzodlar tomonidan mashqlarni bajarish boshlanishidan oldin ularga mashqlarni bajarishga qoʻyiladigan talablar, mashqlarni bajarish tartibi hamda baholash mezonlari Doimiy komissiya aʼzolari tomonidan tushuntiriladi.
38. Nomzodlar tomonidan bajarilishi lozim boʻlgan jismoniy tayyorgarlik mashqlari va ularni bajarishga qoʻyiladigan talablar erkak va ayol jinsidagi nomzodlar uchun alohida belgilanadi. 


-§. Nomzodlarni psixologik oʻrganish
43. Nomzodlarni psixologik oʻrganish test va topshiriqlar (bundan buyon matnda test deb yuritiladi) shaklida amalga oshiriladi.
44. Test savollari Doimiy komissiya raisi tomonidan tasdiqlanadi.
45. Milliy gvardiya Kadrlar boshqarmasining masʼul xodimi test savollari va ularning javoblari saqlanishi hamda oshkor qilinmasligi uchun javobgar hisoblanadi.
46. Test sinovlari mazkur Nizomning 8-ilovasiga muvofiq Psixologik test sinovlarini oʻtkazish metodikasi va baholash mezonlari asosida oʻtkaziladi va baholanadi. 
47. Psixologik oʻrganish natijalari qoniqarsiz boʻlgan nomzod test sinovini u baholangan kundan boshlab 1 yil davomida 2 marotaba qayta topshirishi mumkin, biroq shu kunning oʻzida qayta topshirishga yoʻl qoʻyilmaydi. 

Nomzodni tibbiy koʻrikdan oʻtkazish
55. Nomzodlar sogʻligʻining holatiga koʻra kontrakt boʻyicha harbiy xizmatni oʻtashga yaroqli yoki yaroqsiz ekanligini aniqlash maqsadida ular tibbiy koʻrikdan oʻtkaziladi.
56. Nomzodlarga tibbiy koʻrikdan oʻtish uchun yoʻllanmalar Milliy gvardiya boʻlinmalari tibbiyot xizmatlari tomonidan beriladi.
57. Saralashning dastlabki oʻrganish, jismoniy tayyorgarlik darajasini baholash va psixologik oʻrganish bosqichlaridan muvaffaqiyatli oʻtgan nomzodlar garnizon (hududiy) harbiy-tibbiy komissiyalariga tibbiy koʻrikdan oʻtish uchun yuboriladi

Maxsus antropometrik parametrlar tatbiq etilmaydigan mutaxassisliklar 
ROʻYXATI
I. Quyidagi mutaxassisliklar boʻyicha oliy maʼlumotga ega boʻlganlar:
1. Xalqaro munosabatlar va tashqi siyosat.
2. Sanʼat yoʻnalishida: aktyorlik sanʼati, vokal sanʼati, cholgʻu ijrochiligi, xoreografiya.
3. Kino-teleoperatorlik.
4. Psixologiya.
5. Jurnalistika.
6. Moliya.
7. Yurisprudensiya.
8. Portlatish ishi.
9. Kompyuter texnologiyalari va informatika.
10. Arxitektura va qurilish.
11. Aloqa va axborotlashtirish, telekommunikatsiya texnologiyalari.
12. Veterinariya.
13. Davolash ishi, tibbiy profilaktika ishi, farmatsiya. 
II. Quyidagi mutaxassisliklar boʻyicha oʻrta-maxsus maʼlumotga ega boʻlganlar:
1. Sanʼat yoʻnalishida: vokal sanʼati, cholgʻu ijrochiligi, xoreografiya.
2. Veterinariya.


Jismoniy tayyorgarlik mashqlarini bajarishda nomzodlar quyidagi yosh guruhlariga boʻlinadi: 
1-guruh: 25 yoshgacha boʻlganlar; 
2-guruh: 25 yoshdan — 30 yoshgacha boʻlganlar; 
3-guruh: 30 yoshdan — 35 yoshgacha boʻlganlar; 
4-guruh: 35 yoshdan — 40 yoshgacha boʻlganlar; 
5-guruh: 40 yoshdan — 45 yoshgacha boʻlganlar; 
6-guruh: 45 yoshdan — 50 yoshgacha boʻlganlar; 
7-guruh: 50 yoshdan — 55 yoshgacha boʻlganlar


Quyidagilar erkak jinsidagi nomzodlar tomonidan bajarilishi lozim boʻlgan jismoniy tayyorgarlik mashqlari hisoblanadi: 
toʻsinda (turnikda) tortilish yoki tiralib yotgan holda qoʻlni bukish va rostlash*; 
100 metrga yugurish**; 
3000 yoki 1000 metrga yugurish***. 
8. Quyidagilar ayol jinsidagi nomzodlar tomonidan bajarilishi lozim boʻlgan jismoniy tayyorgarlik mashqlari hisoblanadi: 
kuch ishlatish mashqlari kompleksi; 
100 metrga yugurish****; 
1000 metrga yugurish. 
9. Jismoniy tayyorgarlik mashqlarini bajarish oraligʻida nomzodlarga dam olish uchun kamida 20 daqiqa vaqt beriladi. 
. Jismoniy tayyorgarlik mashqlarini uzrsiz sabablarga koʻra bajarmagan, shuningdek mashqlarni bajarishi “qoniqarsiz” baho bilan baholangan nomzodlarga jismoniy tayyorgarlik mashqlarini shu kunning oʻzida qayta topshirishga ruxsat berilmaydi. 
“Qoniqarsiz” baho bilan baholangan nomzod, jismoniy tayyorgarlik darajasini baholash mashqlarini u baholangan kundan boshlab 1 yil davomida 2 marotaba qayta topshirishi mumkin, biroq shu kunning oʻzida qayta topshirishga yoʻl qoʻyilmaydi. 


Nomzodlarning jismoniy tayyorgarlik darajasini baholash mezonlari 
1. Erkak jinsidagi nomzodlar uchun:
Mashqning nomlanishi	Oʻlchov birligi	Yosh guruhi	Baholash mezonlari
			Aʼlo	Yaxshi	Qoniqarli
Toʻsinda (turnikda) tortilish	marta	1	14	12	10
		2	13	11	9
		3	12	10	8
		4	10	8	6
		5	8	6	4
		6	7	5	3
		7	6	4	2
Tiralib yotgan holatda qoʻllarni bukish va rostlash	marta	1	40	35	30
		2	35	30	25
		3	30	25	20
		4	25	20	18
		5	22	18	16
		6	20	16	14
		7	18	14	12
100 metrga yugurish	sekund	1	14,0	14,4	15,2
		2	14,2	14,6	15,4
		3	14,4	14,8	15,6
		4	14,8	15,4	16,0
		5	15,4	16,0	16,6
3000 metrga yugurish	daqiqa, sekund	1	12,40	13,00	13,35
		2	13,00	13,20	13,55
		3	13,20	13,40	14,10
		4	14,20	14,50	15,30
1000 metrga yugurish	daqiqa, sekund	5	4,15	4,35	5,00
		6	4,25	5,00	5,30
		7	5,10	5,40	6,00
2. Ayol jinsidagi nomzodlar uchun:
Mashqning nomlanishi	Oʻlchov birligi	Yosh guruhi	Baholash mezonlari
			Aʼlo	Yaxshi	Qoniqarli
Kompleks kuch ishlatish mashqlari	marta	1	32	29	26
		2	30	27	24
		3	28	25	22
		4	25	22	19
		5	23	20	17
		6	21	18	15
		7	17	14	11
100 metrga yugurish	sekund	1	16,8	17,4	18,0
		2	17,4	18,0	18,6
		3	18,0	18,6	19,2
		4	18,6	19,6	20,6
1000 metrga yugurish	daqiqa, sekund	1	4,25	4,40	5,00
		2	4,35	4,50	5,20
		3	4,50	5,05	5,40
		4	5,10	5,30	6,00
		5	5,20	5,40	6,10
		6	5,30	5,50	6,20
		7	6,00	6,10	6,30

"""

THANKS_RE = re.compile(r"^(katta\s+)?(rahmat|raxmat|tashakkur|спасибо|thanks?( you)?|thx)[\s!.)]*$", re.I)
THANKS_REPLY = "Arziydi! Yana savolingiz bo'lsa, bemalol murojaat qiling."

TG_LIMIT = 4096
EMBLEM_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets", "emblem.jpg")
emblem_file_id: str | None = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("mg_bot")

dp = Dispatcher()
histories: dict[int, deque] = defaultdict(lambda: deque(maxlen=HISTORY_LIMIT * 2))
user_locks: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
http: aiohttp.ClientSession | None = None


def init_knowledge_db():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS knowledge (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name TEXT NOT NULL,
                file_hash TEXT NOT NULL,
                chunk_no INTEGER NOT NULL,
                content TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_knowledge_hash ON knowledge(file_hash)")
        conn.commit()


def split_into_chunks(text: str, chunk_size: int = 1100, overlap: int = 180) -> list[str]:
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    if not text:
        return []
    chunks = []
    step = max(1, chunk_size - overlap)
    for pos in range(0, len(text), step):
        piece = text[pos:pos + chunk_size].strip()
        if piece:
            chunks.append(piece)
        if pos + chunk_size >= len(text):
            break
    return chunks

def extract_text_from_file(path: Path) -> str:
    ext = path.suffix.lower()
    if ext == ".pdf":
        from pypdf import PdfReader
        reader = PdfReader(str(path))
        pages = [page.extract_text() or "" for page in reader.pages]
        text = "\n".join(pages).strip()
        if text:
            return text
        try:
            import fitz
            from PIL import Image
            import pytesseract
            import io
            doc = fitz.open(str(path))
            ocr_pages = []
            for page in doc:
                pix = page.get_pixmap(matrix=fitz.Matrix(2, 2), alpha=False)
                image = Image.open(io.BytesIO(pix.tobytes("png")))
                ocr_pages.append(pytesseract.image_to_string(image, lang=os.getenv("OCR_LANG", "uzb+eng+rus")))
            return "\n".join(ocr_pages).strip()
        except Exception as exc:
            raise ValueError("PDF ichidagi matn ajratilmadi.") from exc

    if ext == ".docx":
        from docx import Document
        doc = Document(str(path))
        parts = [p.text for p in doc.paragraphs if p.text.strip()]
        for table in doc.tables:
            for row in table.rows:
                parts.append(" | ".join(cell.text.strip() for cell in row.cells))
        return "\n".join(parts).strip()

    if ext == ".txt":
        return path.read_text(encoding="utf-8", errors="replace").strip()

    if ext in {".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tif", ".tiff"}:
        from PIL import Image
        import pytesseract
        image = Image.open(str(path))
        return pytesseract.image_to_string(image, lang=os.getenv("OCR_LANG", "uzb+eng+rus")).strip()

    raise ValueError("Bu fayl turi qo'llab-quvvatlanmaydi.")


def index_document(path: Path, original_name: str) -> tuple[int, str]:
    content = extract_text_from_file(path)
    if len(content.strip()) < 10:
        raise ValueError("Fayldan yetarli matn o'qilmadi.")
    file_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
    chunks = split_into_chunks(content)
    with sqlite3.connect(DB_PATH) as conn:
        exists = conn.execute("SELECT 1 FROM knowledge WHERE file_hash=? LIMIT 1", (file_hash,)).fetchone()
        if exists:
            return 0, "Bu hujjat avval qo'shilgan."
        conn.executemany(
            "INSERT INTO knowledge(file_name, file_hash, chunk_no, content) VALUES (?, ?, ?, ?)",
            [(original_name, file_hash, i, chunk) for i, chunk in enumerate(chunks)]
        )
        conn.commit()
    return len(chunks), ""



def _merge_overlap(left: str, right: str) -> str:
    max_ov = min(len(left), len(right), 220)
    for ov in range(max_ov, 40, -1):
        if left[-ov:] == right[:ov]:
            return left + right[ov:]
    return left + "\n" + right


def find_relevant_chunks(question: str, limit: int = 6) -> list[tuple[str, str]]:
    q_lower = for_search(question)

    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT file_name, chunk_no, content FROM knowledge ORDER BY file_name, chunk_no"
        ).fetchall()

    matches_found = re.findall(
        r"(\d+)\s*[-–—.]?\s*(modda|band|qism)",
        q_lower,
    )

    keyed: dict[tuple[str, int], str] = {}
    scored: list[tuple[float, str, int]] = []
    stop = {
        "ning", "nima", "qanday", "uchun", "bilan", "yoki", "hamda", "yana",
        "shu", "bu", "va", "ham", "deb", "edi", "ekan",
    }

    words = [
        w
        for w in re.findall(r"[a-zA-Z0-9']{4,}", q_lower)
        if w not in stop
    ]
    words = list(dict.fromkeys(words))

    for file_name, chunk_no, content in rows:
        keyed[(file_name, chunk_no)] = content
        content_lower = for_search(content)
        score = 0.0

        for num, term in matches_found:
            if re.search(rf"{re.escape(num)}\s*[-–—.]\s*{term}", content_lower) or re.search(
                rf"{re.escape(num)}\s+{term}", content_lower
            ):
                score += 25.0

        if words:
            matches = sum(1 for word in words if word in content_lower)
            score += matches / len(words)

        if score > 0:
            scored.append((score, file_name, chunk_no))

    scored.sort(key=lambda item: item[0], reverse=True)

    def _has_article(content: str) -> bool:
        content_lower = for_search(content)
        for num, term in matches_found:
            if re.search(rf"{re.escape(num)}\s*[-–—.]\s*{term}", content_lower) or re.search(
                rf"{re.escape(num)}\s+{term}", content_lower
            ):
                return True
        return False

    article_files: set[str] = set()
    picked: list[tuple[str, int]] = []
    seen: set[tuple[str, int]] = set()
    for _, file_name, chunk_no in scored[:limit]:
        if matches_found and _has_article(keyed[(file_name, chunk_no)]):
            article_files.add(file_name)
        for n in range(chunk_no - 1, chunk_no + 2):
            key = (file_name, n)
            if key in keyed and key not in seen:
                seen.add(key)
                picked.append(key)

    file_order: list[str] = []
    for file_name, _chunk_no in picked:
        if file_name in article_files and file_name not in file_order:
            file_order.append(file_name)
    for file_name, _chunk_no in picked:
        if file_name not in file_order:
            file_order.append(file_name)

    merged: list[tuple[str, str]] = []
    for file_name in file_order:
        nos = sorted(n for f, n in picked if f == file_name)
        buf = ""
        last_no = None
        for chunk_no in nos:
            text = keyed[(file_name, chunk_no)]
            if last_no is not None and chunk_no == last_no + 1:
                buf = _merge_overlap(buf, text)
            else:
                if buf:
                    merged.append((file_name, buf))
                buf = text
            last_no = chunk_no
        if buf:
            merged.append((file_name, buf))
    return merged

# def find_relevant_chunks(question: str, limit: int = 5) -> list[tuple[str, str]]:
#     q_lower = to_latin(question).lower()
#
#     with sqlite3.connect(DB_PATH) as conn:
#         rows = conn.execute("SELECT file_name, content FROM knowledge").fetchall()
#
#     matches_found = re.findall(r"(\d+)\s*[-–—]?\s*(modda|band|qism|yil)", q_lower)
#
#     # Muhim so'zlarni ajratib olish (qisqartirmasdan yoki to'liqroq olib)
#     words = [w for w in re.findall(r"[a-zA-ZА-Яа-яЁёЎўҚқҒғҲҳ0-9']{3,}", q_lower)]
#     words = list(set(words))
#
#     scored = []
#
#     for file_name, content in rows:
#         content_lower = to_latin(content).lower()
#         score = 0.0
#
#         # Aniq raqamli mosliklar (masalan, yillar yoki moddalar)
#         for num, term in matches_found:
#             if num in content_lower:
#                 score += 5.0
#
#         # So'zlar bo'yicha moslikni tekshirish
#         if words:
#             matches = sum(1 for word in words if word in content_lower)
#             score += matches / len(words)
#
#         if score > 0:
#             scored.append((score, file_name, content))
#
#     scored.sort(key=lambda item: item[0], reverse=True)
#     return [(file_name, content) for score, file_name, content in scored[:limit]]



async def ask_ollama(user_id: int, text: str) -> str:
    history = histories[user_id]
    relevant = find_relevant_chunks(text)

    if relevant:
        context = "YUKLANGAN TO'LIQ HUJJATLARDAN TOPILGAN PARCHALAR:\n"
        for i, (file_name, chunk) in enumerate(relevant, 1):
            context += f"\n[Manba {i} - {file_name}]:\n{chunk}\n"
        context += (
            "\nQAT'IY QO'LLANMA: Yuqoridagi hujjat parchalariga tayanibgina javob ber. "
            "Savolga oid barcha muhim band, ro'yxat va faktlarni qisqartirmasdan, "
            "oxirigacha yozib ber. Javobni jumla o'rtasida to'xtatma."
        )
    else:
        context = "TIZIMDA BU MAVZU BO'YICHA HUJJAT TOPILMADI."

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        *history,
        {"role": "user", "content": f"{context}\n\nFOYDALANUVCHI SAVOLI:\n{text}"}
    ]
    # payload = {
    #     "model": OLLAMA_MODEL,
    #     "messages": messages,
    #     "stream": False,
    #     "options": {"temperature": 0.0, "top_p": 0.8, "repeat_penalty": 1.1, "num_ctx": 8192},
    # }
    # ask_ollama funksiyasidagi payload ichida:
    payload = {
        "model": OLLAMA_MODEL,
        "messages": messages,
        "stream": False,
        "options": {
            "temperature": 0.1,
            "top_p": 0.9,
            "repeat_penalty": 1.1,
            "num_ctx": 16384,
            "num_predict": 2500
        },
    }

    pieces: list[str] = []
    for _ in range(3):
        payload["messages"] = messages
        async with http.post(f"{OLLAMA_URL}/api/chat", json=payload) as resp:
            resp.raise_for_status()
            data = await resp.json()
        piece = ((data.get("message") or {}).get("content") or "").strip()
        if piece:
            pieces.append(piece)
        if data.get("done_reason") != "length" or not piece:
            break
        messages = [
            *messages,
            {"role": "assistant", "content": piece},
            {
                "role": "user",
                "content": "Javobingiz kesilib qoldi. Qolganini oxirigacha yozing, boshidan takrorlamang.",
            },
        ]

    answer = clean_answer("\n".join(pieces))
    history.append({"role": "user", "content": text})
    history.append({"role": "assistant", "content": answer})
    return answer


CYR2LAT = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "yo", "ж": "j", "з": "z", "и": "i",
    "й": "y", "к": "k", "л": "l", "м": "m", "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t",
    "у": "u", "ф": "f", "х": "x", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "sh", "ъ": "'", "ы": "i", "ь": "",
    "э": "e", "ю": "yu", "я": "ya", "ў": "o'", "қ": "q", "ғ": "g'", "ҳ": "h",
}


def to_latin(text: str) -> str:
    def conv(ch: str) -> str:
        lat = CYR2LAT.get(ch.lower())
        if lat is None:
            return ch
        return lat[:1].upper() + lat[1:] if ch.isupper() else lat

    return "".join(conv(ch) for ch in text)


def normalize_apostrophes(text: str) -> str:
    return re.sub(r"[ʻʼ‘’`´]", "'", text)


def new_to_old_latin(text: str) -> str:
    """ç, ş, ğ, ö ni qidiruv uchun eski yozuvga (ch, sh, g', o') qaytaradi."""
    out = []
    for ch in text:
        if ch == "ç":
            out.append("ch")
        elif ch == "Ç":
            out.append("Ch")
        elif ch == "ş":
            out.append("sh")
        elif ch == "Ş":
            out.append("Sh")
        elif ch == "ğ":
            out.append("g'")
        elif ch == "Ğ":
            out.append("G'")
        elif ch == "ö":
            out.append("o'")
        elif ch == "Ö":
            out.append("O'")
        else:
            out.append(ch)
    return "".join(out)


def _case_letter(src: str, new_char: str) -> str:
    if src.isupper():
        return new_char.upper()
    if src[0].isupper():
        return new_char.upper()
    return new_char.lower()


def old_to_new_latin(text: str) -> str:
    """Eski lotin yozuvini yangi alifboga o'tkazadi: ch→ç, sh→ş, g'→ğ, o'→ö."""
    text = normalize_apostrophes(text)
    text = re.sub(r"[Cc][Hh]", lambda m: _case_letter(m.group(0), "ç"), text)
    text = re.sub(r"[Ss][Hh]", lambda m: _case_letter(m.group(0), "ş"), text)
    text = re.sub(r"[Gg]'", lambda m: _case_letter(m.group(0)[0], "ğ"), text)
    text = re.sub(r"[Oo]'", lambda m: _case_letter(m.group(0)[0], "ö"), text)
    return text


def for_search(text: str) -> str:
    """Savol va hujjatlarni bir xil shaklda solishtirish (eski va yangi harflar teng)."""
    return new_to_old_latin(normalize_apostrophes(to_latin(text))).lower()


# def clean_answer(text: str) -> str:
#     disclaimer = (
#         "Bu bot maslahat xususiyatiga ega, rasmiy murojaat uchun "
#         "1215 qo'ng'iroq qilishingiz yoki milliygvardiya.uz saytiga murojaat qilishingiz mumkin!"
#     )
#
#     # Model javobida oldindan bo'lsa, olib tashlash
#     text = re.sub(re.escape(disclaimer), "", text, flags=re.I)
#
#     text = re.sub(r"__(.+?)__", r"\1", text)
#     text = re.sub(r"^\s*[*•]\s+", "- ", text, flags=re.M)
#     text = re.sub(r"^#{1,6}\s*", "", text, flags=re.M)
#     text = re.sub(r"^(\d+\.)\s{2,}", r"\1 ", text, flags=re.M)
#     text = re.sub(r"[ʻʼ‘’`]", "'", text)
#     text = to_latin(text)
#     text = re.sub(
#         r"\b([Pp])olitsiya",
#         lambda m: ("M" if m.group(1) == "P" else "m") + "ilitsiya",
#         text,
#     )
#
#     return text.strip() + "\n\n" + disclaimer
def clean_answer(text: str) -> str:
    disclaimer = (
        "Bu bot maslahat xususiyatiga ega, rasmiy murojaat uchun "
        "1215 qo'ng'iroq qilishingiz yoki milliygvardiya.uz saytiga murojaat qilishingiz mumkin!"
    )

    text = text.replace(disclaimer, "").strip()
    text = to_latin(text)

    # Faqat kerakli tozalashlarni qoldiring
    text = re.sub(r"__(.+?)__", r"\1", text)
    text = re.sub(r"\*\*(.+?)\*\*", r"*\1*", text)
    text = re.sub(r"^\s*[*•]\s+", "- ", text, flags=re.M)
    text = re.sub(r"^#{1,6}\s*", "", text, flags=re.M)
    text = re.sub(r"[ʻʼ‘’`]", "'", text)

    return old_to_new_latin(text.strip() + "\n\n" + disclaimer)

# def split_text(text: str, limit: int = TG_LIMIT) -> list[str]:
#     parts = []
#     while len(text) > limit:
#         cut = text.rfind("\n", 0, limit)
#         if cut <= 0:
#             cut = limit
#         parts.append(text[:cut])
#         text = text[cut:].lstrip("\n")
#     if text:
#         parts.append(text)
#     return parts
def split_text(text: str, limit: int = 4000) -> list[str]:  # 4096 dan sal kamroq olamiz
    parts = []
    while len(text) > limit:
        # Matnni gap oxiridan (nuqta, undov, so'roq) kesishga harakat qilamiz
        cut = text.rfind(". ", 0, limit)
        if cut == -1:
            cut = text.rfind("\n", 0, limit)
        if cut == -1:
            cut = limit

        parts.append(text[:cut + 1].strip())
        text = text[cut + 1:].strip()

    if text:
        parts.append(text)
    return parts


async def send_answer_parts(message: Message, text: str, reply_markup=None) -> None:
    """Telegram Markdown yopilmagan * yoki _ ni qabul qilmaydi; xato bo'lsa oddiy matn yuboriladi."""
    text = old_to_new_latin(text)
    for part in split_text(text):
        try:
            await message.answer(part, parse_mode="Markdown", reply_markup=reply_markup)
        except TelegramBadRequest:
            await message.answer(part, parse_mode=None, reply_markup=reply_markup)

@dp.message(CommandStart())
async def cmd_start(message: Message):
    global emblem_file_id
    histories.pop(message.from_user.id, None)
    document_mode[message.from_user.id] = False
    sent = await message.answer_photo(
        photo=emblem_file_id or FSInputFile(EMBLEM_PATH),
        caption=f"Assalomu alaykum, {message.from_user.full_name}! 👋\n\n"
                "Men Milliy gvardiya faoliyatiga oid savollaringizga yordam beraman.\n\n"
                "/start — suhbatni boshlash\n"
                "/yangi — suhbatni boshidan boshlash",
        reply_markup=faq_keyboard(),
    )
    emblem_file_id = sent.photo[-1].file_id


@dp.message(Command("yangi", "reset"))
async def cmd_reset(message: Message):
    histories.pop(message.from_user.id, None)
    document_mode[message.from_user.id] = False
    await message.answer("Suhbat tarixi tozalandi. Yangi savolingizni yozing! ✨")


@dp.message(Command("yordam", "help"))
async def cmd_help(message: Message):
    await message.answer("Buyruqlar:\n/yangi — suhbatni tozalash\n/hujjatlar — yuklangan hujjatlarni ko'rish")


@dp.message(Command("hujjatlar"))
async def cmd_documents(message: Message):
    document_mode[message.from_user.id] = True
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT file_name, COUNT(*) FROM knowledge GROUP BY file_name ORDER BY file_name").fetchall()
    if not rows:
        await message.answer(
            "Hozircha bilimlar omboriga hujjat qo'shilmagan.\n\nEndi sizga fayl yoki rasm yuborishga ruxsat berildi.")
        return
    lines = ["Bilimlar omboridagi hujjatlar:", "\nEndi sizga fayl yoki rasm yuborishga ruxsat berildi."]
    lines.extend(f"- {name} ({count} bo'lak)" for name, count in rows[:50])
    await message.answer("\n".join(lines))


@dp.message(Command("hujjatlarni_tozalash"))
async def cmd_clear_documents(message: Message):
    document_mode[message.from_user.id] = True
    await message.answer("Barcha hujjatlarni o'chirish uchun /tasdiqlash_tozalash buyrug'ini yuboring.")


@dp.message(Command("tasdiqlash_tozalash"))
async def cmd_confirm_clear_documents(message: Message):
    document_mode[message.from_user.id] = True
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM knowledge")
        conn.commit()
    await message.answer("Bilimlar omboridagi hujjatlar o'chirildi.")


@dp.message(F.document)
async def on_document(message: Message, bot: Bot):
    if not document_mode[message.from_user.id]:
        await message.answer("Kechirasiz, fayl jo'natish taqiqlangan. Avval /hujjatlar buyrug'ini yuboring.")
        return

    doc = message.document
    if not doc:
        return
    original_name = Path(doc.file_name or "yuborilgan_fayl").name
    ext = Path(original_name).suffix.lower()
    if ext not in SUPPORTED_EXTENSIONS:
        await message.answer("Bu fayl turi qo'llab-quvvatlanmaydi.")
        return
    if doc.file_size and doc.file_size > MAX_FILE_MB * 1024 * 1024:
        await message.answer(f"Fayl hajmi {MAX_FILE_MB} MB dan oshmasligi kerak.")
        return

    await message.answer("Fayl qabul qilindi. Matnini o'qib, bazaga qo'shyapman...")
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=ext, dir=UPLOAD_DIR) as tmp:
            temp_path = Path(tmp.name)
        tg_file = await bot.get_file(doc.file_id)
        await bot.download_file(tg_file.file_path, destination=temp_path)
        count, note = await asyncio.to_thread(index_document, temp_path, original_name)
        if note:
            await message.answer(note)
        else:
            await message.answer(f"Hujjat bazaga qo'shildi. Bo'laklar soni: {count}.")
    except Exception as exc:
        log.exception("Xatolik")
        await message.answer(f"Xatolik: {str(exc)[:700]}")
    finally:
        if temp_path:
            try:
                temp_path.unlink(missing_ok=True)
            except Exception:
                pass


@dp.message(F.photo)
async def on_photo(message: Message):
    if not document_mode[message.from_user.id]:
        await message.answer("Kechirasiz, rasm yuborish taqiqlangan. Avval /hujjatlar buyrug'ini yuboring.")
        return
    await message.answer("Rasm qabul qilindi, lekin faqat matnli xabarlar qabul qilinadi.")


@dp.message(F.voice | F.audio)
async def on_audio(message: Message):
    await message.answer("Kechirasiz, audio xabarlar qabul qilinmaydi. Matn ko'rinishida yozing.")

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

def feedback_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Qoniqarli", callback_data="feedback:satisfied"),
            InlineKeyboardButton(text="❌ Qoniqarsiz", callback_data="feedback:unsatisfied"),
        ]
    ])
from aiogram import F
from aiogram.types import CallbackQuery

FAQ_QUESTIONS: dict[str, str] = {
    "faq:1": "Milliy gvardiya qachon tashkil etilgan?",
    "faq:2": "Milliy gvardiyaning vazifalari nimalar?",
    "faq:3": "Nomzodlar qanday tanlanadi?",
    "faq:4": "Jismoniy tayyorgarlik darajasi qanday?",
    "faq:5": "Kontrakt bo'yicha xizmat qanday?",
}


def faq_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Milliy gvardiya qachon\ntashkil etilgan?", callback_data="faq:1"),
            InlineKeyboardButton(text="Milliy gvardiyaning\nvazifalari nimalar?", callback_data="faq:2"),
        ],
        [
            InlineKeyboardButton(text="Nomzodlar qanday\ntanlanadi?", callback_data="faq:3"),
            InlineKeyboardButton(text="Jismoniy tayyorgarlik\ndarajasi qanday?", callback_data="faq:4"),
        ],
        [
            InlineKeyboardButton(text="Kontrakt bo'yicha\nxizmat qanday?", callback_data="faq:5"),
        ],
    ])


@dp.callback_query(F.data.startswith("faq:"))
async def handle_faq(callback: CallbackQuery):
    question = FAQ_QUESTIONS.get(callback.data, "Savolingizni yozing.")
    user_id = callback.from_user.id
    lock = user_locks[user_id]
    if lock.locked():
        await callback.answer("Oldingi savolingizga javob tayyorlanyapti, biroz kuting...", show_alert=True)
        return
    await callback.answer()
    async with lock, ChatActionSender.typing(bot=callback.bot, chat_id=callback.message.chat.id):
        try:
            answer = await ask_ollama(user_id, question)
        except Exception:
            log.exception("Javob olishda xatolik")
            await callback.message.answer("Kechirasiz, xatolik yuz berdi.")
            return
    answer_text = answer or "Kechirasiz, javob topa olmadin."
    await send_answer_parts(
        callback.message,
        f"{question}\n\n{answer_text}",
        reply_markup=feedback_keyboard(),
    )


@dp.callback_query(F.data.startswith("feedback:"))
async def handle_feedback(callback: CallbackQuery):
    if callback.data == "feedback:satisfied":
        text = "✅ Fikringiz uchun rahmat!"
    else:
        text = "❌ Fikringiz uchun rahmat!"

    await callback.answer(text, show_alert=True)

@dp.message(F.text)
async def on_text(message: Message, bot: Bot):
    user_id = message.from_user.id
    if THANKS_RE.match(message.text.strip()):
        await message.answer(THANKS_REPLY)
        return
    lock = user_locks[user_id]
    if lock.locked():
        await message.answer("Oldingi savolingizga javob tayyorlanyapti, biroz kuting... ⏳")
        return
    async with lock, ChatActionSender.typing(bot=bot, chat_id=message.chat.id):
        try:
            answer = await ask_ollama(user_id, message.text)
        except Exception:
            log.exception("Javob olishda xatolik")
            await message.answer("Kechirasiz, xatolik yuz berdi.")
            return

    await send_answer_parts(
        message,
        answer or "Kechirasiz, javob topa olmadim.",
        reply_markup=feedback_keyboard(),
    )


@dp.message()
async def on_other(message: Message):
    await message.answer("Faqat matnli xabarlar qabul qilinadi.")


async def main():
    global http
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN topilmadi")
    init_knowledge_db()
    http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300))
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=None))
    log.info("Bot ishga tushdi. Model: %s", OLLAMA_MODEL)
    try:
        await dp.start_polling(bot)
    finally:
        await http.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())