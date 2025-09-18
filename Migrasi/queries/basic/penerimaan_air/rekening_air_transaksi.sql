select
@idpdam as idpdam,
pm2.id as idpelangganair,
pm.id as idperiode,
concat('LPP-69',um.idloket,date_format(tr.TGLBAYAR,'%d%m%Y%H%i%S')) nomortransaksi,
1 as statustransaksi,
tr.TGLBAYAR as waktutransaksi,
year(tr.TGLBAYAR) as tahuntransaksi,
um.id as iduser,
um.idloket as idloket,
null as idkolektiftransaksi,
null as idalasanbatal,
null as keterangan,
tr.TGLBAYAR as waktuupdate
from tbl_rekair tr
join pdam_bulungan_dataawal.pelanggan_map pm2 on pm2.nosamb=tr.nosamb
join pdam_bulungan_dataawal.user_map um on um.kasir=tr.KASIR
join pdam_bulungan_dataawal.periode_map pm on pm.periode=concat(tr.TAHUN,lpad(tr.BULAN,2,'0'))
where tr.TGLBAYAR is not null and tr.KASIR is not null