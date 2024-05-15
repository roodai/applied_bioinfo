import sqlite3
# the filename of this SQLite database
gwascat = "/proj/applied_bioinformatics/users/x_ronod/applied_bioinfo/pre_assignment_6/gwascat.db"
joins = "/proj/applied_bioinformatics/users/x_ronod/applied_bioinfo/pre_assignment_6/joins.db"

# initialize database connection
conn = sqlite3.connect(gwascat)

c = conn.cursor()
c.execute("ATTACH DATABASE ? AS joins", (joins,))

def execute_and_print(command):
  c1=command
  c.execute(c1)
  rows = c.fetchall()
  if c.description:
    column_name = [description[0] for description in c.description]
    print(column_name)
  for row in rows:
    print(row)

queries=[]
queries.append("SELECT trait, strongest_risk_snp, pvalue FROM gwascat ORDER BY pvalue DESC LIMIT 5;")
queries.append("SELECT chrom, position, trait, strongest_risk_snp, pvalue FROM gwascat WHERE strongest_risk_snp = 'rs429358' ;")
queries.append("SELECT chrom, position, trait, strongest_risk_snp, pvalue FROM gwascat WHERE lower(strongest_risk_snp) = 'rs429358';")
queries.append("SELECT chrom, position, trait, strongest_risk_snp, pvalue FROM gwascat WHERE chrom = '22' AND pvalue < 10e-15;")
queries.append("SELECT chrom, position, trait, strongest_risk_snp, pvalue FROM gwascat ORDER BY pvalue LIMIT 5")
queries.append("SELECT chrom, position, trait, strongest_risk_snp, pvalue FROM gwascat WHERE pvalue IS NOT NULL ORDER BY pvalue LIMIT 5;")
queries.append("SELECT chrom, position, strongest_risk_snp, pvalue FROM gwascat WHERE (chrom = '1' OR chrom = '2' OR chrom = '3') AND pvalue < 10e-11 ORDER BY pvalue LIMIT 5;")
queries.append('SELECT chrom, position, strongest_risk_snp, pvalue FROM gwascat WHERE chrom IN ("1", "2", "3") AND pvalue < 10e-11 ORDER BY pvalue LIMIT 5;')
queries.append('SELECT chrom, position, strongest_risk_snp, pvalue FROM gwascat WHERE chrom = "22" AND position BETWEEN 24000000 AND 25000000 AND pvalue IS NOT NULL ORDER BY pvalue LIMIT 5;')
queries.append('SELECT lower(trait) AS trait, "chr" || chrom || ":" || position AS region FROM gwascat LIMIT 5;')
queries.append('SELECT ifnull(chrom, "NA") AS chrom, ifnull(position, "NA") AS position, strongest_risk_snp, ifnull(pvalue, "NA") AS pvalue FROM gwascat WHERE strongest_risk_snp = "rs429358";')
queries.append('SELECT count(*) FROM gwascat;')
queries.append('SELECT count(pvalue) FROM gwascat')
queries.append('SELECT count(*) - count(pvalue) AS number_of_null_pvalues FROM gwascat;')
queries.append('select "2007" AS year, count(*) AS number_entries from gwascat WHERE date BETWEEN "2007-01-01" AND "2008-01-01";')
queries.append('SELECT count(DISTINCT strongest_risk_snp) AS unique_rs FROM gwascat;')
queries.append('SELECT chrom, count(*) FROM gwascat GROUP BY chrom;')
#Bonus
#queries.append('SELECT chrom, count(*) AS chrom_count FROM gwascat GROUP BY chrom;')
queries.append('SELECT chrom, count(*) as nhits FROM gwascat GROUP BY chrom ORDER BY nhits DESC;')
queries.append('select strongest_risk_snp, count(*) AS count  FROM gwascat GROUP BY strongest_risk_snp ORDER BY count DESC LIMIT 5;')
queries.append('select strongest_risk_snp, strongest_risk_allele, count(*) AS count FROM gwascat GROUP BY strongest_risk_snp, strongest_risk_allele ORDER BY count DESC LIMIT 10;')
queries.append('SELECT substr(date, 1, 4) AS year FROM gwascat GROUP BY year;')
queries.append('SELECT substr(date, 1, 4) AS year, round(avg(pvalue_mlog), 4) AS mean_log_pvalue, count(pvalue_mlog) AS n FROM gwascat GROUP BY year;')
queries.append('SELECT substr(date, 1, 4) AS year, round(avg(pvalue_mlog), 4) AS mean_log_pvalue, count(pvalue_mlog) AS n FROM gwascat GROUP BY year HAVING count(pvalue_mlog) > 10;')
queries.append('SELECT substr(date, 1, 4) AS year, author, pubmedid, count(*) AS num_assoc FROM gwascat GROUP BY pubmedid LIMIT 5;')
queries.append('SELECT year, avg(num_assoc) FROM (SELECT substr(date, 1, 4) AS year, author, count(*) AS num_assoc FROM gwascat GROUP BY pubmedid) GROUP BY year;')
queries.append('SELECT date, pubmedid, author, strongest_risk_snp FROM gwascat WHERE pubmedid = "24388013" LIMIT 5;')
queries.append('SELECT * FROM assocs')
queries.append('SELECT * FROM studies')
queries.append('SELECT * FROM assocs INNER JOIN studies ON assocs.study_id = studies.id')
queries.append('SELECT studies.id, assocs.id, trait, year FROM assocs INNER JOIN studies ON assocs.study_id = studies.id;')
queries.append('SELECT studies.id AS study_id, assocs.id AS assoc_id, trait, year FROM assocs INNER JOIN studies ON assocs.study_id = studies.id')
queries.append('SELECT count(*) FROM assocs INNER JOIN studies ON assocs.study_id = studies.id;')
queries.append('SELECT count(*) FROM assocs')
queries.append('SELECT * FROM assocs WHERE study_id NOT IN (SELECT id FROM studies);')
queries.append('SELECT * FROM studies WHERE id NOT IN (SELECT study_id FROM assocs);')
queries.append('SELECT * FROM assocs LEFT OUTER JOIN studies ON assocs.study_id = studies.id')
queries.append('SELECT * FROM studies LEFT OUTER JOIN assocs ON assocs.study_id = studies.id;')
#'.schema study' does not workr eplaced with "PRAGMA table_info(study)")
queries.append("PRAGMA table_info(study)")

for q in queries:
  execute_and_print(q)
  print("\n")

c.close()
conn.close()

ter_queries=[]
ter_queries.append('CREATE TABLE variants(id integer primary key,chrom text,start integer,end integer,strand text,name text);')
ter_queries.append('SELECT * FROM variants;')
ter_queries.append('INSERT INTO variants(id, chrom, start, end, strand, name) VALUES(NULL, "16", 48224287, 48224287, "+", "rs17822931");')
ter_queries.append('SELECT * FROM variants;')

db_filename = "variants.db"
conn = sqlite3.connect(db_filename)
c = conn.cursor()

for t_q in ter_queries:
    execute_and_print(t_q)
    
c.close()
conn.close()

db_filename = "joins.db"
conn = sqlite3.connect(db_filename)
c = conn.cursor()

id_queries = []
id_queries.append('CREATE INDEX snp_idx ON assocs(strongest_risk_snp)')
id_queries.append('PRAGMA index_list(assocs)')
id_queries.append('CREATE INDEX study_id_idx ON assocs(study_id);')
id_queries.append('PRAGMA index_list(assocs)')
id_queries.append('DROP INDEX snp_idx')
id_queries.append('PRAGMA index_list(assocs)')

for i_q in id_queries:
    execute_and_print(i_q)
    
conn = sqlite3.connect(db_filename)
c = conn.cursor()