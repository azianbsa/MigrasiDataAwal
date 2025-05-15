SELECT
@idpdam,
@id:=@id+1 AS idperiode,
1 AS inisiasi,
0 AS STATUS,
DATE_ADD(STR_TO_DATE(CONCAT(periode, '01'), '%Y%m%d'), INTERVAL 1 MONTH) AS tglmulaitagih,
IFNULL(tglmulaidenda, DATE_ADD(STR_TO_DATE(CONCAT(periode, '21'), '%Y%m%d'), INTERVAL 1 MONTH)) AS tglmulaidenda1,
IFNULL(tglmulaidenda2, DATE_ADD(STR_TO_DATE(CONCAT(periode, '21'), '%Y%m%d'), INTERVAL 2 MONTH)) AS tglmulaidenda2,
IFNULL(tglmulaidenda3, DATE_ADD(STR_TO_DATE(CONCAT(periode, '21'), '%Y%m%d'), INTERVAL 3 MONTH)) AS tglmulaidenda3,
IFNULL(tglmulaidenda4, DATE_ADD(STR_TO_DATE(CONCAT(periode, '21'), '%Y%m%d'), INTERVAL 4 MONTH)) AS tglmulaidenda4,
IFNULL(tglmulaidendaperbulan, DATE_ADD(STR_TO_DATE(CONCAT(periode, '21'), '%Y%m%d'), INTERVAL 1 MONTH)) AS tglmulaidendaperbulan,
IFNULL(tglmulaidenda, DATE_ADD(STR_TO_DATE(CONCAT(periode, '21'), '%Y%m%d'), INTERVAL 1 MONTH)) AS tglmulaidendaperhari,
NOW() AS waktuupdate
FROM
periode
,(SELECT @id:=0) AS id
WHERE periode BETWEEN 202502 AND 202504
ORDER BY periode;