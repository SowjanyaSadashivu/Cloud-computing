A = LOAD '$G' USING PigStorage(',') AS (User : int, rating : double);
A = FILTER A BY rating > 0;
B = GROUP A BY User; 
C = FOREACH B GENERATE (int)(AVG(A.rating)*10) AS avgerage_rating;
D = FOREACH C GENERATE avgerage_rating,1 as counts; 
E = GROUP D BY avgerage_rating;
F = FOREACH E GENERATE (float)group/10, SUM(D.counts) AS counts;
STORE F INTO '$O' USING PigStorage('	');
