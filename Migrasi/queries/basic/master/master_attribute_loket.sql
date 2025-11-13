select
@idpdam as idpdam,
lm.id as idloket,
lm.loket as kodeloket,
lm.loket as namaloket,
lm.idwilayah as idwilayah,
lm.flagmitra as flagmitra,
lm.admmitra as biayamitra,
lm.aktif as status,
null as idbank,
0 as flaghapus,
now() as waktuupdate
from [dataawal].loket_map lm