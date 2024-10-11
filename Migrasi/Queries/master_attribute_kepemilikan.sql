DROP TABLE IF EXISTS _kepemilikan;

CREATE TABLE _kepemilikan (
 idpdam SMALLINT (6) NOT NULL,
 idkepemilikan INT (11) NOT NULL,
 kodekepemilikan VARCHAR (8),
 namakepemilikan VARCHAR (50),
 flaghapus TINYINT (1) DEFAULT 0,
 waktuupdate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY (idpdam, idkepemilikan)
) ENGINE = INNODB;

INSERT INTO _kepemilikan(idpdam,idkepemilikan,kodekepemilikan,namakepemilikan) VALUES
(@idpdam,-1,'-','-'),
(@idpdam,1,'1','Pribadi'),
(@idpdam,2,'2','Sewa/Kontrak'),
(@idpdam,3,'3','Desa'),
(@idpdam,4,'4','Pemda'),
(@idpdam,5,'5','Pemprov'),
(@idpdam,6,'6','Negara');

SELECT
 idpdam,
 idkepemilikan,
 kodekepemilikan,
 namakepemilikan,
 flaghapus,
 waktuupdate
FROM
 _kepemilikan
 WHERE idpdam=@idpdam;
 
DROP TABLE _kepemilikan;