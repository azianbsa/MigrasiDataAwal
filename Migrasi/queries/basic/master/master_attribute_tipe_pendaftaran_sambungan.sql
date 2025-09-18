select
@idpdam as idpdam,
tsm.id as idtipependaftaransambungan,
tsm.program as namatipependaftaransambungan,
0 as flaghapus,
now() as waktuupdate
from [dataawal].tipe_sambungan_map tsm