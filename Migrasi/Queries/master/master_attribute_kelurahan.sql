DROP TEMPORARY TABLE IF EXISTS __tmp_kecamatan;
CREATE TEMPORARY TABLE __tmp_kecamatan AS
SELECT
@id:=@id+1 AS id,
kodekecamatan
FROM
kecamatan
,(SELECT @id:=0) AS id;

SELECT
@idpdam AS idpdam,
@id:=@id+1 AS idkelurahan,
k.kodekelurahan AS kodekelurahan,
k.kelurahan AS namakelurahan,
kc.id AS idkecamatan,
0 AS jumlahjiwa,
0 AS flaghapus,
NOW() AS waktuupdate
FROM
kelurahan k
JOIN __tmp_kecamatan kc ON kc.kodekecamatan = k.kodekecamatan
,(SELECT @id:=0) AS id
WHERE k.`kodekelurahan`<>'-'
UNION ALL
SELECT
@idpdam,
-1 AS idkelurahan,
'-' AS kodekelurahan,
'-' AS namakelurahan,
-1 AS idkecamatan,
0 AS jumlahjiwa,
0 AS flaghapus,
NOW() AS waktuupdate