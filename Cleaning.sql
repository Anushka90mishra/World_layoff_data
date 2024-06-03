-- 1. REMOVE DUPLICATES, 2. STANDARDIZE THE DATA, 3. NULL VALUES OR BLANK, 4. REMOVE IRRELEVANT COLUMN

RENAME TABLE layoffs TO layoffs_raw;

-- data copied from raw table to staging:

create table layoffs_staging
like layoffs_raw;

insert layoffs_staging
select * from layoffs_raw;

select * from layoffs_staging;

-- 1. REMOVE DUPLICATES

select *,
row_number() over(partition by  company,location,industry, total_laid_off,
 percentage_laid_off,`date`,
 stage,country,funds_raised_millions) as row_num
from layoffs_staging;

with duplicate_cte as (
select *,
row_number() over(partition by  company,location,industry, total_laid_off,
 percentage_laid_off,`date`, stage,
 country,funds_raised_millions) as row_num
from layoffs_staging
)
select * from duplicate_cte
where row_num> 1;

select *
from layoffs_staging
where company = 'Casper';

CREATE TABLE `layoffs_staging_2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_staging_2;

insert into layoffs_staging_2
select *,
row_number() over(partition by  company,location,industry, total_laid_off,
 percentage_laid_off,`date`, stage,
 country,funds_raised_millions) as row_num
from layoffs_staging;

select * 
from layoffs_staging_2
where row_num > 1;
-- to off the safe mode
SET SQL_SAFE_UPDATES = 0 ;
-- deleted all duplicates
delete 
from layoffs_staging_2
where row_num > 1;

-- 2. STANDARDIZE THE DATA

-- to remove spaces in front 
select company,(trim(company))
from layoffs_staging_2;

update layoffs_staging_2
set company = trim(company);

select distinct industry 
from layoffs_staging_2
order by 1;

-- standardize the crypto and crypto currency same as they are the same thing in industry:
select * 
from layoffs_staging_2
where industry like 'Crypto%';

update layoffs_staging_2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct industry 
from layoffs_staging_2
order by 1;

select distinct country
from layoffs_staging_2
order by 1;

select distinct country, trim(trailing '.' from country)
from layoffs_staging_2
order by 1;

update layoffs_staging_2
set country = trim(trailing '.' from country)
where country like 'United States%';


-- convert date text into date format
select `date`, 
str_to_date(`date` , '%m/%d/%Y') 
from layoffs_staging_2;

update layoffs_staging_2
set date = str_to_date(`date` , '%m/%d/%Y');

alter table layoffs_staging_2
modify column `date` date; -- coverted into date datatype

select * from layoffs_staging_2;

-- remove null/blank values

select * from layoffs_staging_2
where total_laid_off  is NULL
and percentage_laid_off is null;

select *
from layoffs_staging_2
where industry is null
or industry = '';

update layoffs_staging_2
set industry = null
where industry ='';

SELECT 
    t1.industry, t2.industry
FROM
    layoffs_staging_2 t1
        JOIN
    layoffs_staging_2 t2 ON t1.company = t2.company
WHERE
    (t1.industry IS NULL OR t1.industry = '')
        AND t2.industry IS NOT NULL;

UPDATE layoffs_staging_2 t1
        JOIN
    layoffs_staging_2 t2 ON t1.company = t2.company 
SET 
    t1.industry = t2.industry
WHERE
    (t1.industry IS NULL)
        AND t2.industry IS NOT NULL;  

select * from layoffs_staging_2;

-- Remove Unnecessary Columns/Rows:

select * from layoffs_staging_2
where total_laid_off  is NULL
and percentage_laid_off is null;

delete
from layoffs_staging_2
where total_laid_off  is NULL
and percentage_laid_off is null;

-- drop column row_num

alter table layoffs_staging_2
drop column row_num;










