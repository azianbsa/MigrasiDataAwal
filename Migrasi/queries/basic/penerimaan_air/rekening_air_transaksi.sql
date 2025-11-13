select
@idpdam as idpdam,
pm.idpelangganair as idpelangganair,
pm2.idperiode as idperiode,
b.nolpp nomortransaksi,
b.flaglunas as statustransaksi,
b.tglbayar as waktutransaksi,
year(b.tglbayar) as tahuntransaksi,
um.id as iduser,
um.idloket as idloket,
null as idkolektiftransaksi,
null as idalasanbatal,
null as keterangan,
b.tglbayar as waktuupdate
from bayar b
join [dataawal].pelanggan_map pm on pm.nosamb=b.nosamb
join [dataawal].periode_map pm2 on pm2.kodeperiode=b.periode
join [dataawal].user_map um on um.nama=b.kasir