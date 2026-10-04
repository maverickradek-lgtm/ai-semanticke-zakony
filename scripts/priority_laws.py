"""
Sdileny seznam PRIORITNICH predpisu (klicove zakony pro praci Radka na MF) -
pouziva reshard_and_clean_zakony.py (nastaveni embed_priority pri migraci) a
sync_esbirka_text_neon.py (priorita u novych/zmenenych predpisu) a
embed_zakony_neon.py (faze 0 - nejdriv embedovat tyto predpisy).

Dve urovne:
  PRIORITY_EMBED_PRIORITY (1000)  - vsechna znei prioritniho predpisu (i historicka)
  SUPER_EMBED_PRIORITY    (5000)  - jen AKTUALNI zneni (is_current=true) - embedduje se
                                   uplne PRVNI, napric vsemi shardy, nez cokoli jineho.
Format: (cislo, rok) Sb. - presnejsi nez fuzzy shoda na nazvu.
"""

PRIORITY_PREDPISY = {
    (262, 2006),  # zakonik prace
    (89, 2012),   # obcansky zakonik
    (40, 2009),   # trestni zakonik
    (141, 1961),  # trestni rad
    (119, 2002),  # zakon o strelnych zbranich a strelivu
    (283, 2021),  # stavebni zakon (novy, ucinny od 2024)
    (183, 2006),  # stavebni zakon (stary, pro historicka zneni)
    (361, 2000),  # zakon o provozu na pozemnich komunikacich (silnicni provoz)
    (255, 2012),  # zakon o kontrole (kontrolni rad)
    (320, 2001),  # zakon o financni kontrole ve verejne sprave
    (231, 2025),  # zakon o rizeni a kontrole verejnych financi
    (416, 2004),  # vyhlaska k zakonu o financni kontrole
    (218, 2000),  # rozpoctova pravidla
    (250, 2000),  # rozpoctova pravidla uzemnich rozpoctu
    (420, 2004),  # zakon o prezkoumavani hospodareni USC
    (128, 2000),  # zakon o obcich (obecni zrizeni)
    (129, 2000),  # zakon o krajich (krajske zrizeni)
    (131, 2000),  # zakon o hlavnim meste Praze
    (412, 2021),  # vyhlaska o rozpoctove skladbe
    (433, 2024),  # vyhlaska o financnim vyporadani (aktualni)
    (367, 2015),  # vyhlaska o financnim vyporadani (predchozi)
    (560, 2006),  # vyhlaska o ucasti statniho rozpoctu na financovani programu reprodukce majetku
    (219, 2000),  # zakon o majetku CR
    (62, 2001),   # vyhlaska o hospodareni organizacnich slozek statu
    (134, 2016),  # zakon o zadavani verejnych zakazek
    (340, 2015),  # zakon o registru smluv
    (563, 1991),  # zakon o ucetnictvi
    (410, 2009),  # vyhlaska provadejici zakon o ucetnictvi (vybrane ucetni jednotky)
    (383, 2009),  # technicka vyhlaska o ucetnich zaznamech
    (270, 2010),  # vyhlaska o inventarizaci majetku a zavazku
    (220, 2013),  # vyhlaska o schvalovani ucetnich zaverek
    (280, 2009),  # danovy rad
    (586, 1992),  # zakon o danich z prijmu
    (235, 2004),  # zakon o dani z pridane hodnoty
    (499, 2004),  # zakon o archivnictvi a spisove sluzbe
}

PRIORITY_EMBED_PRIORITY = 1000
SUPER_EMBED_PRIORITY = 5000
