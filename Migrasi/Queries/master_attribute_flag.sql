DROP TABLE IF EXISTS _flag;

CREATE TABLE _flag (
 idpdam SMALLINT (6) NOT NULL,
 idflag INT (11) NOT NULL,
 namaflag VARCHAR (50),
 flaghapus TINYINT (1) DEFAULT 0,
 waktuupdate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY (idpdam, idflag)
) ENGINE = INNODB;

INSERT INTO _flag(idpdam,idflag,namaflag) VALUES
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
 _flag
 WHERE idpdam=@idpdam;
 
DROP TABLE _flag;