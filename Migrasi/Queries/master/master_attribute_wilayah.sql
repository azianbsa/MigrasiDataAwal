SELECT
@idpdam,
@id:=@id+1 AS idwilayah,
kodewil AS kodewilayah,
wilayah AS namawilayah,
0 AS flagpusat,
0 AS flaghapus,
NOW() AS waktuupdate
FROM
wilayah
,(SELECT @id:=0) AS id;