select
@idpdam as idpdam,
lm.id as idloket,
lm.loket as kodeloket,
lm.loket as namaloket,
lm.idwilayah as idwilayah,
0 as flagmitra,
0 as biayamitra,
1 as status,
null as idbank,
0 as flaghapus,
now() as waktuupdate
from [dataawal].loket_map lm