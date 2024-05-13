#!/bin/bash -l
#SBATCH --output=/home/x_ronod/logs/%A_%a.out
#SBATCH --error=/home/x_ronod/logs/%A_%a.error
#SBATCH -t 24:00:00
#SBATCH -N1
#SBATCH --account=naiss2024-22-540


template_command="/proj/applied_bioinformatics/tools/ncbi-blast-2.15.0+-src/blastp -query /proj/applied_bioinformatics/users/x_ronod/applied_bioinfo/pre_assignment_5/protein.fasta"
out_path="/proj/applied_bioinformatics/users/x_ronod/applied_bioinfo/pre_assignment_5/"
db_path="/proj/applied_bioinformatics/common_data/proteomes/"


#should probably loop through unique databases and use those as names for output

$template_command -out $out_path"UP000000589_result" -db $db_path"UP000000589" &
$template_command -out $out_path"UP000000625_result" -db $db_path"UP000000625" &
$template_command -out $out_path"UP000000803_result" -db $db_path"UP000000803" &
$template_command -out $out_path"UP000006548_result" -db $db_path"UP000006548" 
wait

