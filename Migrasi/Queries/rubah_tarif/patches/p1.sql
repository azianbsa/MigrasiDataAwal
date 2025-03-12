REPLACE INTO `permohonan_pelanggan_air_ba` (idpdam,`idpermohonan`,`nomorba`,`tanggalba`,`iduser`)
SELECT idpdam,`idpermohonan`,'-',`waktupermohonan`,-1
FROM `permohonan_pelanggan_air`
WHERE idpdam=@idpdam AND `statuspermohonan`='Menunggu Verifikasi' AND `idtipepermohonan`=@idtipe