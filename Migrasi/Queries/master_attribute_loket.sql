SELECT
 @idpdam,
 @id:=@id+1 AS idloket,
 kodeloket,
 loket AS namaloket,
 NULL AS idwilayah,
 flagmitra,
 admmitra AS biayamitra,
 aktif AS STATUS,
 NULL AS idbank,
 0 AS flaghapus,
 NOW() AS waktuupdate
FROM
 loket
 ,(SELECT @id:=0) AS id;