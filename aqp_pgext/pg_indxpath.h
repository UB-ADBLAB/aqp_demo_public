#ifndef PG_INDXPATH_H
#define PG_INDXPATH_H

#include <nodes/pg_list.h>
#include <nodes/pathnodes.h>

/* Data structure for collecting qual clauses that match an index */
typedef struct
{
	bool		nonempty;		/* True if lists are not all empty */
	/* Lists of IndexClause nodes, one list per index column */
	List	   *indexclauses[INDEX_MAX_KEYS];
} IndexClauseSet;

extern void aqp_pgport_match_restriction_clauses_to_index(PlannerInfo *root,
                                                    IndexOptInfo *index,
                                                    IndexClauseSet *clauseset);
extern void aqp_pgport_match_join_clauses_to_index(PlannerInfo *root,
                                      RelOptInfo *rel, IndexOptInfo *index,
                                      IndexClauseSet *clauseset,
                                      List **joinorclauses);
extern void aqp_pgport_match_eclass_clauses_to_index(PlannerInfo *root,
                                IndexOptInfo *index,
                                IndexClauseSet *clauseset);
extern void aqp_pgport_match_clauses_to_index(PlannerInfo *root,
                       List *clauses,
                       IndexOptInfo *index,
                       IndexClauseSet *clauseset);
extern void aqp_pgport_match_clause_to_index(PlannerInfo *root,
                      RestrictInfo *rinfo,
                      IndexOptInfo *index,
                      IndexClauseSet *clauseset);
extern IndexClause *aqp_pgport_match_clause_to_indexcol(PlannerInfo *root,
                                    RestrictInfo *rinfo,
                                    int indexcol,
                                    IndexOptInfo *index);
extern IndexClause *aqp_pgport_match_boolean_index_clause(RestrictInfo *rinfo,
                           int indexcol,
                           IndexOptInfo *index);
extern IndexClause *aqp_pgport_match_opclause_to_indexcol(PlannerInfo *root,
                           RestrictInfo *rinfo,
                           int indexcol,
                           IndexOptInfo *index);
extern IndexClause *aqp_pgport_match_funcclause_to_indexcol(PlannerInfo *root,
                             RestrictInfo *rinfo,
                             int indexcol,
                             IndexOptInfo *index);
extern IndexClause *aqp_pgport_get_index_clause_from_support(PlannerInfo *root,
                              RestrictInfo *rinfo,
                              Oid funcid,
                              int indexarg,
                              int indexcol,
                              IndexOptInfo *index);
extern IndexClause *aqp_pgport_match_saopclause_to_indexcol(RestrictInfo *rinfo,
                             int indexcol,
                             IndexOptInfo *index);
extern IndexClause *aqp_pgport_match_rowcompare_to_indexcol(RestrictInfo *rinfo,
                             int indexcol,
                             IndexOptInfo *index);
extern IndexClause *aqp_pgport_expand_indexqual_rowcompare(RestrictInfo *rinfo,
                            int indexcol,
                            IndexOptInfo *index,
                            Oid expr_op,
                            bool var_on_left);
extern bool aqp_pgport_ec_member_matches_indexcol(PlannerInfo *root,
                            RelOptInfo *rel,
						    EquivalenceClass *ec, EquivalenceMember *em,
						    void *arg);
extern double pg_port_approximate_joinrel_size(PlannerInfo *root, Relids relids);
extern double pg_port_adjust_rowcount_for_semijoins(PlannerInfo *root,
							  Index cur_relid,
							  Index outer_relid,
							  double rowcount);
extern double pg_port_get_loop_count(PlannerInfo *root, Index cur_relid,
                                     Relids outer_relids);
extern bool pg_port_check_index_only(RelOptInfo *rel, IndexOptInfo *index);

#endif  /* PG_INDXPATH_H */
