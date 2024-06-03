--  Exploratory Data Analysis:

select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging_2;

select *
from layoffs_staging_2
where percentage_laid_off = 1;

select company, sum(total_laid_off)
from layoffs_staging_2
group by company
order by 2 desc;

select min(`date`), max(`date`) 
from layoffs_staging_2;

select industry, sum(total_laid_off)
from layoffs_staging_2
group by industry
order by 2 desc;

select country, sum(total_laid_off)
from layoffs_staging_2
group by country
order by 2 desc;

select year(`date`), sum(total_laid_off)
from layoffs_staging_2
group by year(`date`)
order by 1 desc;

-- progression of lay_offs

select substring(`date`,1,7) as `Month` , sum(total_laid_off)
from layoffs_staging_2
where substring(`date`,1,7) is not null
group by `Month`
order by 1;

-- rolling sum of data based on the year and month

with Rolling_Total  as
(select substring(`date`,1,7) as `Month` , sum(total_laid_off) as Laid_off
from layoffs_staging_2
where substring(`date`,1,7) is not null
group by `Month`
order by 1
)
select `Month`, Laid_off
, sum(Laid_off) over(order by `Month`) as rolling_total
from Rolling_Total;

-- highest 5 rank company based on the lay_off in diffrent years

select company, year(`date` ),sum(total_laid_off)
from layoffs_staging_2
group by company, year(`date` )
order by 3 desc;

with Company_year (company, years, total_laid_off) as 
(select company, year(`date` ),sum(total_laid_off)
from layoffs_staging_2
group by company, year(`date` )
),
Company_year_rank as
(select *, dense_rank() over (partition by years order by total_laid_off desc) as Ranking
from Company_year
where years is not null
)
select *
from Company_year_rank
where Ranking <= 5;



