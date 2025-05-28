/* rekening_air_detail
 * new(0, "idpdam")
 * new(1, "idpelangganair")
 * new(2, "idperiode")
 * new(3, "blok1")
 * new(4, "blok2")
 * new(5, "blok3")
 * new(6, "blok4")
 * new(7, "blok5")
 * new(8, "prog1")
 * new(9, "prog2")
 * new(10, "prog3")
 * new(11, "prog4")
 * new(12, "prog5")
 */

SELECT
@idpdam,
p.id AS idpelangganair,
pr.idperiode AS idperiode,
IFNULL(r.blok1, 0) AS blok1,
IFNULL(r.blok2, 0) AS blok2,
IFNULL(r.blok3, 0) AS blok3,
IFNULL(r.blok4, 0) AS blok4,
IFNULL(r.blok5, 0) AS blok5,
IFNULL(r.prog1, 0) AS prog1,
IFNULL(r.prog2, 0) AS prog2,
IFNULL(r.prog3, 0) AS prog3,
IFNULL(r.prog4, 0) AS prog4,
IFNULL(r.prog5, 0) AS prog5
FROM piutang r
JOIN pelanggan p ON p.nosamb = r.nosamb
JOIN `kotaparepare_dataawal`.`master_periode` pr ON pr.`kodeperiode` = r.periode AND pr.`idpdam`=@idpdam
WHERE r.periode BETWEEN 202502 AND 202504