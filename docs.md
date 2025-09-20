# Global conventions

- **Lowercase SQL**: all keywords, functions, aliases.
- **No subqueries in FROM**: use CTEs only.
- **One responsibility per CTE**: raw intake, filters, aggregations, joins, or presentation. Not mixed.
- **Stable parameter binding**: first CTEs bind params.
- **Joins**: prefer `left join` for optional sources, `join` for required.
- **Aggregations**: aggregate in dedicated `agg_*` CTEs only.

### Select layout
```sql
select
  field_1,
  field_2,
  field_3
from table
join other using key
