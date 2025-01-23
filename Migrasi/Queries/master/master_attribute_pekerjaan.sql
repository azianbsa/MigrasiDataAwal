DROP TABLE IF EXISTS __tmp_pekerjaan;

CREATE TABLE __tmp_pekerjaan (
 idpdam SMALLINT (6) NOT NULL,
 idpekerjaan INT (11) NOT NULL,
 namapekerjaan VARCHAR (50),
 flaghapus TINYINT (1) DEFAULT 0,
 waktuupdate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY (idpdam, idpekerjaan)
) ENGINE = INNODB;

INSERT INTO __tmp_pekerjaan(idpdam,idpekerjaan,namapekerjaan) VALUES
(@idpdam,-1,'-'),
(@idpdam,0,'Lainnya'),
(@idpdam,1,'Swasta'),
(@idpdam,2,'Wiraswasta'),
(@idpdam,3,'PNS'),
(@idpdam,4,'Pedagang'),
(@idpdam,5,'BUMN'),
(@idpdam,6,'BUMD'),
(@idpdam,7,'Kuliah'),
(@idpdam,8,'Pelajar'),
(@idpdam,9,'Nelayan');

SELECT
 idpdam,
 idpekerjaan,
 namapekerjaan,
 flaghapus,
 waktuupdate
FROM
 __tmp_pekerjaan
 WHERE idpdam=@idpdam;
 
DROP TABLE __tmp_pekerjaan;