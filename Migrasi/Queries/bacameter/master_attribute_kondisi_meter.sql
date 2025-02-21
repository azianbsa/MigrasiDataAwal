SELECT
@idpdam,
@id:=@id+1 AS idkondisimeter,
kodekondisi AS kodekondisimeter,
kondisi AS namakondisimeter,
0 AS flaghapus,
NOW() AS waktuupdate
FROM
kondisimeter
,(SELECT @id:=0) AS id;