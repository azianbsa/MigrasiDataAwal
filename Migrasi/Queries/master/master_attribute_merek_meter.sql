SELECT
@idpdam,
@id:=@id+1 AS idmerekmeter,
@id AS kodemerekmeter,
merk AS namamerekmeter,
0 AS flaghapus,
NOW() AS waktuupdate
FROM
merkmeter,
(SELECT @id:=0) AS id
WHERE merk<>'-'
UNION ALL
SELECT
@idpdam,
-1 AS idmerekmeter,
'-' AS kodemerekmeter,
'-' AS namamerekmeter,
0 AS flaghapus,
NOW() AS waktuupdate