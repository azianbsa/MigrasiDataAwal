SELECT
@idpdam AS idpdam,
@id:=@id+1 AS idpegawai,
nip AS nomorindukpegawai,
nama AS namapegawai,
alamat AS alamatktp,
1 AS flagaktif,
0 AS flaghapus,
NOW() AS waktuupdate
FROM pegawai
,(SELECT @id:=0) AS id