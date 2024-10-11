DROP TABLE IF EXISTS _kwh;

CREATE TABLE _kwh (
 idpdam SMALLINT (6) NOT NULL,
 idkwh INT (11) NOT NULL,
 kodekwh VARCHAR (8),
 namakwh VARCHAR (50),
 flaghapus TINYINT (1) DEFAULT 0,
 waktuupdate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY (idpdam, idkwh)
) ENGINE = INNODB;

INSERT INTO _kwh(idpdam,idkwh,kodekwh,namakwh) VALUES
(@idpdam,1,'01','450 VA'),
(@idpdam,2,'02','900 VA'),
(@idpdam,3,'03','1.300 VA'),
(@idpdam,4,'04','2.200 VA'),
(@idpdam,5,'05','3.500 VA'),
(@idpdam,6,'06','4.400 VA'),
(@idpdam,7,'07','5.500 VA'),
(@idpdam,8,'08','6.600 VA');

SELECT
 idpdam,
 idkwh,
 kodekwh,
 namakwh,
 flaghapus,
 waktuupdate
FROM
 _kwh
 WHERE idpdam=@idpdam;
 
DROP TABLE _kwh;