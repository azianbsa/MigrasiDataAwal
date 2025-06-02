-- personalia_master_pegawai
-- new(0, "idpdam")
-- new(1, "idpegawai")
-- new(2, "nomorindukpegawai")
-- new(3, "namapegawai")
-- new(4, "alamatktp")
-- new(5, "flagaktif")
-- new(6, "flaghapus")
-- new(7, "waktuupdate")

SELECT
@idpdam AS `idpdam`,
@id:=@id+1 AS `idpegawai`,
nip AS `nomorindukpegawai`,
nama AS `namapegawai`,
alamat AS `alamatktp`,
1 AS `flagaktif`,
0 AS `flaghapus`,
NOW() AS `waktuupdate`
FROM pegawai
,(SELECT @id:=0) AS id