SELECT
@idpdam,
@id:=@id+1 AS idperiode,
LEFT(periode,4) AS tahun,
periode AS kodeperiode,
nama AS namaperiode,
0 AS flaghapus,
NOW() AS waktuupdate
FROM
periode
,(SELECT @id:=0) AS id
ORDER BY periode;