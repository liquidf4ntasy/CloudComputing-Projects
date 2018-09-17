/** CSE 6331: Project 5 - Matrix Multiplication using Pig
  *  Name: Satyajit Deshmukh
  *  UTA ID: 1001417727
  */

M_matrix = LOAD 'M-matrix-large.txt' USING PigStorage(',') AS (row,col,value);

N_matrix = LOAD 'N-matrix-large.txt' USING PigStorage(',') AS (row,col,value);

Value1 = JOIN M_matrix BY col FULL OUTER, N_matrix BY row;
Multiply = FOREACH Value1 GENERATE M_matrix::row AS r1, N_matrix::col AS c2, (M_matrix::value)*(N_matrix::value) AS value;
GroupMatrix = GROUP Multiply BY (r1, c2);
Result = FOREACH GroupMatrix GENERATE group.$0 as row, group.$1 as col, SUM(Multiply.value) AS val;
STORE Result INTO 'output';