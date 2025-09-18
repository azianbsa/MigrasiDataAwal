select
@idpdam as idpdam,
spm.id as idsumberpengaduan,
spm.sumber as namasumberpengaduan,
0 as flaghapus,
now() as waktuupdate
from [dataawal].sumber_pengaduan_map spm