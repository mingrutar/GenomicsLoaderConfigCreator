--- pre_fill.sql

--- clean all tables, sqlite does not have truncate
DELETE FROM host;
DELETE FROM template;
DELETE FROM loader_config_tag;

--- fill host TABLE
INSERT INTO host (hostname) VALUES ("compute-2-22");
INSERT INTO host (hostname) VALUES ("compute-2-23");
INSERT INTO host (hostname) VALUES ("compute-2-24");
INSERT INTO host (hostname) VALUES ("compute-2-25");
INSERT INTO host (hostname) VALUES ("compute-2-26");
INSERT INTO host (hostname) VALUES ("compute-2-27");
INSERT INTO host (hostname) VALUES ("compute-2-28");
INSERT INTO host (hostname) VALUES ("compute-2-29");

--- template 
INSERT INTO template (name, file_path) VALUES ("vid", '$WS_HOME/templates/vid.json' );
INSERT INTO template (name, file_path, params, extra) 
VALUES ("callsets", '$WS_HOME/templates/callsets.temp', '{"@data_dir@" : "/scratch/1000genome" }',
    '{"histogram": "$WS_HOME/templates/1000_histogram"}' );
INSERT INTO template (name, file_path) 
VALUES ("vcf_header", '$WS_HOME/templates/template_vcf_header.vcf' );
INSERT INTO template (name, file_path) 
VALUES ("ref_genome", '/data/broad/samples/joint_variant_calling/broad_reference/Homo_sapiens_assembly19.fasta' );

--- loader_config_tag 
INSERT INTO loader_config_tag (name, type, default_value) 
VALUES ("row_based_partitioning", 'Boolean', "false");
INSERT INTO loader_config_tag (name, type, default_value) 
VALUES ("produce_combined_vcf", 'Boolean', "false");
INSERT INTO loader_config_tag (name, type, default_value) 
VALUES ("produce_tiledb_array", 'Boolean', "true");
INSERT INTO loader_config_tag (name, type, default_value) 
VALUES ("vid_mapping_file", 'Template', "vid");
INSERT INTO loader_config_tag (name, type, default_value) 
VALUES ("callset_mapping_file", 'Template', "callsets");
INSERT INTO loader_config_tag (name, type, default_value) 
VALUES ("treat_deletions_as_intervals", 'Boolean', "false");
INSERT INTO loader_config_tag (name, type, default_value) 
VALUES ("vcf_header_filename", 'Template', "vcf_header");
INSERT INTO loader_config_tag (name, type, default_value) 
VALUES ("reference_genome", 'Template', "ref_genome");
INSERT INTO loader_config_tag (name, type, default_value) 
VALUES ("offload_vcf_output_processing", 'Boolean', "true");
INSERT INTO loader_config_tag (name, type, default_value) 
VALUES ("ub_callset_row_idx", 'Number', "999");
INSERT INTO loader_config_tag (name, type, default_value) 
VALUES ("discard_vcf_index", 'Boolean', "true");
INSERT INTO loader_config_tag (name, type, default_value) 
VALUES ("produce_tiledb_array", 'Boolean', "true");
INSERT INTO loader_config_tag (name, type, default_value) 
VALUES ("tiledb_compression_level", 'Number', "6");
INSERT INTO loader_config_tag (name, type, default_value) 
VALUES ("disable_synced_writes", 'Boolean', "true");
INSERT INTO loader_config_tag (name, type, default_value) 
VALUES ("delete_and_create_tiledb_array", 'Boolean', "true");
INSERT INTO loader_config_tag (name, type, default_value) 
VALUES ("ignore_cells_not_in_partition", 'Boolean', "false");

-- for produce_combined_vcf = true ??
INSERT INTO loader_config_tag (name, type, default_value) 
VALUES ("produce_combined_vcf", 'Boolean', "false");
INSERT INTO loader_config_tag (name, type, default_value) 
VALUES ("vcf_output_format", 'String', "z");

-- user definable columns 
INSERT INTO loader_config_tag (name, type, default_value, tag_code, user_definable) 
VALUES ("column_partitions", "make_col_partition()", '1', 'bn', 1); 
-- unit MiB, default 100 MiB
INSERT INTO loader_config_tag (name, type, default_value, tag_code, user_definable) 
VALUES ("size_per_column_partition", 'MB', "100", 'sp', 1); 
INSERT INTO loader_config_tag (name, type, default_value, tag_code, user_definable) 
-- number parallel read to 1, so far 2 didn't improve' 
VALUES ("num_parallel_vcf_files", 'Number', "1", 'pf', 1);
-- default compress. 
INSERT INTO loader_config_tag (name, type, default_value, tag_code, user_definable) 
VALUES ("compress_tiledb_array", 'Boolean', "true", 'c', 1);
-- number cells per tile
INSERT INTO loader_config_tag (name, type, default_value, tag_code, user_definable) 
VALUES ("num_cells_per_tile", 'Number', "1000", 'nt', 1);
---added after feb 17
INSERT INTO loader_config_tag (name, type, default_value, tag_code, user_definable) 
VALUES ("segment_size", 'MB', "10", 'sg', 1);
INSERT INTO loader_config_tag (name, type, default_value, tag_code, user_definable) 
VALUES ("do_ping_pong_buffering", 'Boolean', "true", 'pb', 1);
