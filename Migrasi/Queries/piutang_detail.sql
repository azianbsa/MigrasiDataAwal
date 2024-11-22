DROP TEMPORARY TABLE IF EXISTS temp_dataawal_periode;
CREATE TEMPORARY TABLE temp_dataawal_periode (
    idperiode INT,
    periode VARCHAR(10),
    INDEX idx_temp_dataawal_periode_periode (periode)
);
INSERT INTO temp_dataawal_periode
SELECT
@idperiode:=@idperiode+1 AS idperiode,
periode
FROM periode
,(SELECT @idperiode:=0) AS idperiode
ORDER BY periode;

SELECT
 @idpdam,
 pel.id AS idpelangganair,
 per.idperiode AS idperiode,
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
 piutang rek
 JOIN pelanggan pel ON pel.nosamb = rek.nosamb
 JOIN temp_dataawal_periode per ON per.periode = rek.periode
 WHERE rek.kode = CONCAT(rek.periode, '.', rek.nosamb) AND rek.flagangsur = 0;