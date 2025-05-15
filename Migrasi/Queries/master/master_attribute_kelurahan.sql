DROP TEMPORARY TABLE IF EXISTS __tmp_kecamatan;
CREATE TEMPORARY TABLE __tmp_kecamatan AS
SELECT
@id:=@id+1 AS id,
kodekecamatan
FROM
kecamatan
,(SELECT @id:=0) AS id;

SELECT
@idpdam,
@id:=@id+1 AS idkelurahan,
k.kodekelurahan,
k.kelurahan AS namakelurahan,
kc.id AS idkecamatan,
0 AS jumlahjiwa,
0 AS flaghapus,
NOW() AS waktuupdate
FROM
kelurahan k
JOIN __tmp_kecamatan kc ON kc.kodekecamatan = k.kodekecamatan
,(SELECT @id:=0) AS id