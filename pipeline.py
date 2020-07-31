from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import countDistinct, udf
from pyspark.sql.types import StringType
import pyspark.sql.functions as functions

"""
    Class that computes
"""

class Pipeline:

    def run(self, master, app_name, log_level, input_file, output_dir, jobs):
        conf = SparkConf().setMaster(master).setAppName(app_name)
        sc = SparkContext(conf=conf)
        spark = SparkSession(sc)

        # Determines log level that will be shown on screen
        spark.sparkContext.setLogLevel(log_level.upper())

        # Creates logger
        log4jLogger = sc._jvm.org.apache.log4j
        logger = log4jLogger.LogManager.getLogger(__name__)
        
        # Read inputed files in a folder, parse each line and map them to a spark.sql.Row
        logger.debug('Reading input files')
        nasa_requests_data = spark.sparkContext.textFile(input_file)
 
        logger.debug('Mapping data to columns')
        mapped_nasa_requests = nasa_requests_data\
            .map(self.mapper)            
        
        nasa_requests = spark.createDataFrame(mapped_nasa_requests).cache()
        nasa_requests.createOrReplaceTempView('nasa_requests')
        
        if 'hosts-unicos' in jobs:
            logger.info('Calculating distinct hosts')
            distinct_hosts = nasa_requests\
                .where(nasa_requests.host != 'missing')\
                .agg(countDistinct(nasa_requests.host).alias('distinct_hosts'))
            
            logger.info('Writing distinct hosts result to file')
            self.write_to_file(distinct_hosts, output_dir, app_name, 'hosts-unicos')
            distinct_hosts.show()

        if 'total-404' in jobs:
            logger.info('Calculating total 404 errors')
            total_404 = nasa_requests\
                .where(nasa_requests.response == '404')\
                .agg(functions.count(functions.lit(1)).alias('total_404_erros'))
            
            logger.info('Writing total 404 erros to file')
            self.write_to_file(total_404, output_dir, app_name, 'total_404')
            total_404.show()
            
        if 'top-5-url-404' in jobs:
            logger.info('Calculating top 5 URLs with most 404')
            top_urls_404 = nasa_requests\
                .select(nasa_requests.host)\
                .where((nasa_requests.response == '404') & (nasa_requests.host != 'missing'))\
                .groupBy(nasa_requests.host)\
                .agg(functions.count(functions.lit(1)).alias('404_count'))\
                .orderBy('404_count', ascending=0)\
                .limit(5)
            
            logger.info('Writing top 5 URLs with most 404 to file')
            self.write_to_file(top_urls_404, output_dir, app_name, 'top_5_urls_404')
            top_urls_404.show()
            
        if '404-dia' in jobs:
            logger.info('Calculating count of 404 by day')
            extract_date = udf(lambda timestamp: timestamp[0:11], StringType())

            error_404_day = nasa_requests\
                .withColumn('day', extract_date(nasa_requests.timestamp))\
                .where((nasa_requests.response == '404') & (nasa_requests.timestamp != 'missing'))\
                .groupBy('day')\
                .agg(functions.count(functions.lit(1)).alias('404_count'))

            logger.info('Writing count of 404 by day to file')
            self.write_to_file(error_404_day, output_dir, app_name, 'error_404_per_day')
            error_404_day.show()
            
        if 'total-bytes' in jobs:
            logger.info('Calculating total bytes exchanged')
            total_bytes = nasa_requests\
                .agg(functions.sum(nasa_requests.size).alias('total_bytes'))

            logger.info('Writing total bytes exchanged to file')
            self.write_to_file(total_bytes, output_dir, app_name, 'total-bytes')
            total_bytes.show()
            
    def mapper(self, line):
        line = line.strip()

        host = line[:line.find(' - -')]
        if not host:
            host = 'missing'

        timestamp = line[line.find('[') + 1:line.find(']')]
        if not timestamp:
           timestamp = 'missing'

        request = line[line.find('"') + 1:line.rfind('"')]
        if not request:
            request = 'missing'

        try:
            fields = line.split(' ')
            response = fields[-2]
            if not response:
                response = 'missing'

            try:
                size = int(fields[-1])
            except:
                #self.logger.error(('(Byte) Não foi possível fazer o parsing de: {0}. '
                #    'Igualando a 0.'.format(fields[-1])))
                size = 0
        except:
            response = 'missing'
            size = 0

        return Row(host=host, timestamp=timestamp, request=request, response=response, size=size)

    def write_to_file(self, df, output_dir, app_name, job_dir):
        df.write.csv(output_dir + '/{0}-{1}'.format(app_name, job_dir))