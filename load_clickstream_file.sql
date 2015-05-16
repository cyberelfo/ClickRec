
-- Use the command below to load this file
-- mysql -uroot --local-infile < load_clickstream_file.sql 

-- Check progress with the query below. 
-- SELECT table_rows FROM information_schema.tables WHERE table_name = 'stream';

use stream;

alter table stream drop index filename_ix;
alter table stream drop index product_id_2;
alter table stream drop index product_id;

SET FOREIGN_KEY_CHECKS = 0;
SET UNIQUE_CHECKS = 0;
SET sql_log_bin = 0;

load data local infile '/Users/franklin/Downloads/stream/rt-actions-read-2015_01_14_23.log' 
INTO TABLE stream COLUMNS TERMINATED BY ',' LINES TERMINATED BY '\n'
(`product_id`, `type`, `document_id`, `provider_id`, `user_id`, `timestamp`);

SET UNIQUE_CHECKS = 1;
SET FOREIGN_KEY_CHECKS = 1;
SET sql_log_bin = 1;

update stream
set filename = 'rt-actions-read-2015_01_14_23.log',
stream_datetime = FROM_UNIXTIME(SUBSTR(timestamp,1, 10))
where filename is null;

create index `product_id` on stream (`product_id`,`document_id`,`user_id`);
create index `product_id_2` on stream (`product_id`,`user_id`,`document_id`);
create index `filename_ix` on stream (`filename`,`product_id`,`user_id`);
