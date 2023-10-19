package ast

import (
	"fmt"
)

var (
	ErrMergeNotSupportDDL = fmt.Errorf("can not support ddl")
)

//Merge  other ddl to generate final create sql
func (n *CreateTableStmt) Merge(other DDLNode) error {
	if at, ok := other.(*AlterTableStmt); ok {
		return n.MergeAlterTableStmt(at)
	}
	if at, ok := other.(*CreateIndexStmt); ok {
		tp := ConstraintIndex
		if at.KeyType == IndexKeyTypeUnique {
			tp = ConstraintUniqIndex
		}
		n.Constraints = append(n.Constraints, &Constraint{
			node:         at.node,
			IfNotExists:  at.IfNotExists,
			Tp:           tp,
			Name:         at.IndexName,
			Keys:         at.IndexPartSpecifications,
			Refer:        nil,
			Option:       at.IndexOption,
			Expr:         nil,
			Enforced:     false,
			InColumn:     false,
			InColumnName: "",
			IsEmptyIndex: false,
		})
		return nil
	}
	return ErrMergeNotSupportDDL
}

func (n *CreateTableStmt) MergeAlterTableStmt(at *AlterTableStmt) error {
	for _, v := range at.Specs {
		err := n.mergeAlterTableSpec(v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *CreateTableStmt) mergeAlterTableSpec(spec *AlterTableSpec) error {
	switch spec.Tp {
	case AlterTableOption:
		{
			for _, v := range spec.Options {
				for ii, vv := range n.Options {
					if vv.Tp == v.Tp {
						if v.Tp == TableOptionCharset {
							v.UintValue = TableOptionCharsetWithoutConvertTo
						}
						n.Options[ii] = v
					}
				}
			}
		}
	case AlterTableAddColumns:
		{
			switch spec.Position.Tp {
			case ColumnPositionFirst:
				n.Cols = append(append([]*ColumnDef{}, spec.NewColumns...), n.Cols...)
			case ColumnPositionAfter:
				relativeColumn := spec.Position.RelativeColumn
				for i, v := range n.Cols {
					if v.Name.Name.L == relativeColumn.Name.L {
						n.Cols = append(append(append([]*ColumnDef{}, n.Cols[:i+1]...), spec.NewColumns...), n.Cols[i+1:]...)
						break
					}
				}
			}
		}

	case AlterTableAddConstraint:
		{
			n.Constraints = append(n.Constraints, spec.Constraint)
		}
	case AlterTableDropColumn:
		{
			for i, v := range n.Cols {
				if v.Name.Name.L == spec.OldColumnName.Name.L {
					n.Cols = append(n.Cols[:i], n.Cols[i:]...)
					break
				}
			}
		}
	case AlterTableDropPrimaryKey:
		{
			for i, v := range n.Constraints {
				if v.Tp == ConstraintPrimaryKey {
					n.Constraints = append(n.Constraints[:i], n.Constraints[i+1:]...)
					break
				}
			}
		}
	case AlterTableDropIndex:
		{
			for i, v := range n.Constraints {
				if v.Name == spec.Name {
					n.Constraints = append(n.Constraints[:i], n.Constraints[i+1:]...)
					break
				}
			}
		}
	case AlterTableModifyColumn:
		{
			for _, v := range spec.NewColumns {
				for ii, vv := range n.Cols {
					if vv.Name.Name.L == v.Name.Name.L {
						n.Cols[ii] = v
						break
					}
				}
			}
		}
	case AlterTableChangeColumn:
		{
			oldName := spec.OldColumnName.Name
			for i, v := range n.Cols {
				if v.Name.Name.L == oldName.L {
					n.Cols[i] = spec.NewColumns[0]
					break
				}
			}
		}
	case AlterTableRenameColumn:
		{
			oldName := spec.OldColumnName.Name
			for _, v := range n.Cols {
				if v.Name.Name.L == oldName.L {
					v.Name = spec.NewColumnName
					break
				}
			}
		}
	case AlterTableRenameTable:
		{
			n.Table = spec.NewTable
		}
	case AlterTableAlterColumn:
		{
			for _, v := range spec.NewColumns {
				for ii, vv := range n.Cols {
					if vv.Name.Name.L == v.Name.Name.L {
						n.Cols[ii] = v
						break
					}
				}
			}
		}
	case AlterTableRenameIndex:
		{
			oldKey := spec.FromKey.L
			for _, v := range n.Constraints {
				if v.Tp == ConstraintIndex && v.Name == oldKey {
					v.Name = spec.ToKey.L
					break
				}
			}
		}
	case AlterTableAddPartitions:
		{
			n.Partition.Definitions = append(n.Partition.Definitions, spec.PartDefinitions...)
		}
	case AlterTableDropPartition:
		{
			for _, v := range spec.PartitionNames {
				for ii, vv := range n.Partition.Definitions {
					if vv.Name.L == v.L {
						n.Partition.Definitions = append(n.Partition.Definitions[:ii], n.Partition.Definitions[ii+1:]...)
						break
					}
				}
			}
		}
	case AlterTableReorganizePartition:
		{
			n.Partition.Definitions = spec.PartDefinitions
		}
	default:
		return fmt.Errorf("can not support type:%v", spec.Tp.String())
	}

	return nil
}
