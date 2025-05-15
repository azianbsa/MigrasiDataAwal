DROP TEMPORARY TABLE IF EXISTS __tmp_cabang;
CREATE TEMPORARY TABLE __tmp_cabang AS
SELECT
@id:=@id+1 AS id,
kodecabang
FROM
cabang
,(SELECT @id:=0) AS id;

SELECT
@idpdam,
@id:=@id+1 AS idkecamatan,
k.kodekecamatan,
k.kecamatan AS namakecamatan,
c.id AS idcabang,
0 AS flaghapus,
NOW() AS waktuupdate
FROM
kecamatan k
JOIN __tmp_cabang c ON c.kodecabang = k.kodecabang
,(SELECT @id:=0) AS id