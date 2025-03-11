UPDATE `permohonan_pelanggan_air` a
JOIN __tmp_nonair_baliknama b ON b.nomor=a.`nomorpermohonan`
JOIN `rekening_nonair` c ON c.idpdam=a.`idpdam` AND c.`nomornonair`=b.`urutannonair`
SET a.`idnonair`=c.`idnonair`
WHERE a.`idpdam`=@idpdam AND a.`idtipepermohonan`=@idtipe;

UPDATE permohonan_pelanggan_air a
JOIN `rekening_nonair_transaksi` b ON b.`idpdam`=a.`idpdam` AND b.`idnonair`=a.`idnonair`
JOIN permohonan_pelanggan_air_ba c ON c.idpdam=a.`idpdam` AND c.idpermohonan=a.`idpermohonan`
SET a.`statuspermohonan`=
IF(b.`statustransaksi`=1,
 IF(c.idpermohonan IS NULL,
  'Menunggu Verifikasi',
  'Selesai'),
 'Menunggu Pelunasan Reguler')
WHERE a.idpdam=@idpdam AND a.`idtipepermohonan`=@idtipe AND a.statuspermohonan<>'Selesai';