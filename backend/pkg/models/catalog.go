package models

import (
	"database/sql/driver"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Catalog represents a data catalog (database)
type Catalog struct {
	ID          uuid.UUID      `gorm:"type:uuid;primary_key" json:"id"`
	Name        string         `gorm:"uniqueIndex;not null" json:"name"`
	Description string         `json:"description"`
	Owner       string         `json:"owner"`
	Properties  Properties     `gorm:"type:jsonb" json:"properties"`
	Namespaces  []Namespace    `gorm:"foreignKey:CatalogID" json:"namespaces,omitempty"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
	DeletedAt   gorm.DeletedAt `gorm:"index" json:"-"`
}

// Namespace represents a schema within a catalog
type Namespace struct {
	ID          uuid.UUID      `gorm:"type:uuid;primary_key" json:"id"`
	CatalogID   uuid.UUID      `gorm:"type:uuid;not null" json:"catalog_id"`
	Name        string         `gorm:"not null" json:"name"`
	Description string         `json:"description"`
	Owner       string         `json:"owner"`
	Properties  Properties     `gorm:"type:jsonb" json:"properties"`
	Tables      []Table        `gorm:"foreignKey:NamespaceID" json:"tables,omitempty"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
	DeletedAt   gorm.DeletedAt `gorm:"index" json:"-"`
}

// Table represents a table within a namespace
type Table struct {
	ID               uuid.UUID      `gorm:"type:uuid;primary_key" json:"id"`
	NamespaceID      uuid.UUID      `gorm:"type:uuid;not null" json:"namespace_id"`
	Name             string         `gorm:"not null" json:"name"`
	Description      string         `json:"description"`
	Owner            string         `json:"owner"`
	Format           string         `json:"format"` // delta, iceberg, parquet
	Location         string         `json:"location"`
	Schema           TableSchema    `gorm:"type:jsonb" json:"schema"`
	PartitionColumns []string       `gorm:"type:jsonb" json:"partition_columns"`
	Properties       Properties     `gorm:"type:jsonb" json:"properties"`
	Statistics       Statistics     `gorm:"type:jsonb" json:"statistics"`
	CreatedAt        time.Time      `json:"created_at"`
	UpdatedAt        time.Time      `json:"updated_at"`
	DeletedAt        gorm.DeletedAt `gorm:"index" json:"-"`
}

// TableSchema represents the schema of a table
type TableSchema struct {
	Fields []Field `json:"fields"`
}

// Field represents a column in a table
type Field struct {
	Name        string                 `json:"name"`
	Type        string                 `json:"type"`
	Nullable    bool                   `json:"nullable"`
	Description string                 `json:"description,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// Statistics represents table statistics
type Statistics struct {
	RowCount      int64     `json:"row_count"`
	SizeBytes     int64     `json:"size_bytes"`
	LastAnalyzed  time.Time `json:"last_analyzed"`
	MinValues     map[string]interface{} `json:"min_values,omitempty"`
	MaxValues     map[string]interface{} `json:"max_values,omitempty"`
	NullCounts    map[string]int64      `json:"null_counts,omitempty"`
}

// Properties is a custom type for JSON properties
type Properties map[string]interface{}

// Value implements driver.Valuer interface
func (p Properties) Value() (driver.Value, error) {
	return json.Marshal(p)
}

// Scan implements sql.Scanner interface
func (p *Properties) Scan(value interface{}) error {
	if value == nil {
		*p = make(Properties)
		return nil
	}
	bytes, ok := value.([]byte)
	if !ok {
		return nil
	}
	return json.Unmarshal(bytes, p)
}

// Value implements driver.Valuer for TableSchema
func (s TableSchema) Value() (driver.Value, error) {
	return json.Marshal(s)
}

// Scan implements sql.Scanner for TableSchema
func (s *TableSchema) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	bytes, ok := value.([]byte)
	if !ok {
		return nil
	}
	return json.Unmarshal(bytes, s)
}

// Value implements driver.Valuer for Statistics
func (s Statistics) Value() (driver.Value, error) {
	return json.Marshal(s)
}

// Scan implements sql.Scanner for Statistics
func (s *Statistics) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	bytes, ok := value.([]byte)
	if !ok {
		return nil
	}
	return json.Unmarshal(bytes, s)
}

// BeforeCreate hooks
func (c *Catalog) BeforeCreate(tx *gorm.DB) error {
	if c.ID == uuid.Nil {
		c.ID = uuid.New()
	}
	if c.Properties == nil {
		c.Properties = make(Properties)
	}
	return nil
}

func (n *Namespace) BeforeCreate(tx *gorm.DB) error {
	if n.ID == uuid.Nil {
		n.ID = uuid.New()
	}
	if n.Properties == nil {
		n.Properties = make(Properties)
	}
	return nil
}

func (t *Table) BeforeCreate(tx *gorm.DB) error {
	if t.ID == uuid.Nil {
		t.ID = uuid.New()
	}
	if t.Properties == nil {
		t.Properties = make(Properties)
	}
	return nil
}