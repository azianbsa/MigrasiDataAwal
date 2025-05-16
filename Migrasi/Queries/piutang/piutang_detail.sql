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