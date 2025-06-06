﻿-- master_attribute_flag
-- new(0, "idpdam")
-- new(1, "idflag")
-- new(2, "namaflag")
-- new(3, "flaghapus")
-- new(4, "waktuupdate")

DROP TABLE IF EXISTS __tmp_flag;

CREATE TABLE __tmp_flag (
 idpdam SMALLINT (6) NOT NULL,
 idflag INT (11) NOT NULL,
 namaflag VARCHAR (50),
 flaghapus TINYINT (1) DEFAULT 0,
 waktuupdate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY (idpdam, idflag)
) ENGINE = INNODB;

INSERT INTO __tmp_flag(idpdam,idflag,namaflag) VALUES
(@idpdam,1,'Normal'),
(@idpdam,2,'Terpusat/Tanpa Denda'),
(@idpdam,3,'Air Tidak Mengalir'),
(@idpdam,4,'Hapus Secara Akuntansi'),
(@idpdam,5,'Usulan Hapus Secara Akuntansi');

SELECT
 idpdam,
 idflag,
 namaflag,
 flaghapus,
 waktuupdate
FROM
 __tmp_flag
 WHERE idpdam=@idpdam;
 
DROP TABLE __tmp_flag;