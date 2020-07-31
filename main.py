from datetime import datetime
import argparse

from pipeline import Pipeline


available_jobs = ['hosts-unicos', 'total-404', 'top-5-url-404', '404-dia', 'total-bytes']

def formatted_date():
    return datetime.now().strftime('%Y%m%d-%H%M%S')

def get_jobs(jobs):
    job_list = jobs.lower().split(',')

    if 'todos' in job_list:
        return available_jobs
    
    # Delete jobs/words that are not in available_jobs
    return [job for job in job_list if job in available_jobs]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    
    parser.add_argument(
        '--master',
        dest='master',
        help='Endereço do master que executará o job. O default é local',
        default='local'
    )
    parser.add_argument(
        '--app-name',
        dest='app_name',
        help='Nome do job',
        default='spark-job-{0}'.format(formatted_date())
    )
    parser.add_argument(
        '--input-file',
        dest='input_file',
        help='Arquivo(s) que deseja ler',
        required=True
    )
    parser.add_argument(
        '--output-dir',
        dest='output_dir',
        help='''Pasta na qual as saídas serão escritas.\n
            Cada job cria uma pasta para escrever seus arquivos''',
        required=True
    )
    parser.add_argument(
        '--jobs',
        dest='jobs',
        help='''Jobs que serão executados. Jobs disponíveis: {0}.\n
            Use vírgula para executar mais de um (--jobs=hosts-unicos,total-404).
            Por padrão todos os jobs serão executados.'''.format(', '.join(available_jobs)),
        default='todos'
    )
    parser.add_argument(
        '--log-level',
        dest='log_level',
        choices={'off', 'fatal', 'error', 'warn', 'info', 'debug', 'trace', 'all'},
        help='Determina o level do logger. O padrão é info'
    )

    known_args, _ = parser.parse_known_args()
    
    # Select jobs to be executed
    jobs = get_jobs(known_args.jobs)

    Pipeline().run(known_args.master, known_args.app_name, known_args.log_level, 
        known_args.input_file, known_args.output_dir, jobs)
