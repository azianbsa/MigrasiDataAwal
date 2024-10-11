DROP TABLE IF EXISTS _jnsbangunan;

CREATE TABLE _jnsbangunan (
 idpdam SMALLINT (6) NOT NULL,
 idjenisbangunan INT (11) NOT NULL,
 namajenisbangunan VARCHAR (50),
 flaghapus TINYINT (1) DEFAULT 0,
 waktuupdate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY (idpdam, idjenisbangunan)
) ENGINE = INNODB;

INSERT INTO _jnsbangunan(idpdam,idjenisbangunan,namajenisbangunan) VALUES
(@idpdam,-1,'-'),
(@idpdam,1,'BERTINGKAT'),
(@idpdam,2,'TIDAK BERTINGKAT'),
(@idpdam,3,'RUKO');

SELECT
 idpdam,
 idjenisbangunan,
 namajenisbangunan,
 flaghapus,
 waktuupdate
FROM
 _jnsbangunan
 WHERE idpdam=@idpdam;
 
DROP TABLE _jnsbangunan;