select
@idpdam as idpdam,
um.id as iduser,
um.nama as nama,
um.namauser as namauser,
'$2a$11$RgW5Fv/9uo3SxWyWqOJ7wOLkSZO1STX5dpzkIYaBjLzYA08cJfOFe' as passworduser,
um.flagaktif as aktif,
null as noidentitas,
2 as idrole,
um.idloket as idloket,
null as idpegawai,
0 as flagbatasiwilayahpelayanan,
0 as flaghapus,
now() as waktuupdate
from [dataawal].user_map um