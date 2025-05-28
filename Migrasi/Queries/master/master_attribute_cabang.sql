SELECT
@idpdam,
@id:=@id+1 AS idcabang,
kodecabang,
cabang AS namacabang,
0 AS flaghapus,
NOW() AS waktuupdate
FROM
cabang
,(SELECT @id:=0) AS id
WHERE `kodecabang`<>'-'
UNION ALL
SELECT
@idpdam,
-1 AS idcabang,
'-' AS kodecabang,
'-' AS namacabang,
0 AS flaghapus,
NOW() AS waktuupdate