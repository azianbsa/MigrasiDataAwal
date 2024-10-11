SELECT
 @idpdam,
 @id:=@id+1 AS idpelangganair,
 @id AS idperiode,
 0 AS blok1,
 0 AS blok2,
 0 AS blok3,
 0 AS blok4,
 0 AS blok5,
 0 AS prog1,
 0 AS prog2,
 0 AS prog3,
 0 AS prog4,
 0 AS prog5
FROM
 piutang
 ,(SELECT @id := 0) AS id;