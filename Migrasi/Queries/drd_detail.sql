SELECT
 @idpdam,
 pel.idpelanggan AS idpelangganair,
 per.idperiode,
 IFNULL(rek.blok1, 0) AS blok1,
 IFNULL(rek.blok2, 0) AS blok2,
 IFNULL(rek.blok3, 0) AS blok3,
 IFNULL(rek.blok4, 0) AS blok4,
 IFNULL(rek.blok5, 0) AS blok5,
 IFNULL(rek.prog1, 0) AS prog1,
 IFNULL(rek.prog2, 0) AS prog2,
 IFNULL(rek.prog3, 0) AS prog3,
 IFNULL(rek.prog4, 0) AS prog4,
 IFNULL(rek.prog5, 0) AS prog5
FROM
 drd[tahunbulan] rek
 JOIN (
	SELECT
	@idpelanggan:=@idpelanggan+1 AS idpelanggan,
	nosamb
	FROM pelanggan
	,(SELECT @idpelanggan:=0) AS idpelanggan 
 ) pel ON pel.nosamb = rek.nosamb
 JOIN (
	SELECT
	@idperiode:=@idperiode+1 AS idperiode,
	periode
	FROM periode
	,(SELECT @idperiode:=0) AS idperiode
 ) per ON per.periode = [tahunbulan];