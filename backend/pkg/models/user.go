package models

import (
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

type User struct {
	ID           uuid.UUID      `gorm:"type:uuid;primary_key" json:"id"`
	Email        string         `gorm:"uniqueIndex;not null" json:"email"`
	Username     string         `gorm:"uniqueIndex;not null" json:"username"`
	PasswordHash string         `gorm:"not null" json:"-"`
	FirstName    string         `json:"first_name"`
	LastName     string         `json:"last_name"`
	Roles        []Role         `gorm:"many2many:user_roles;" json:"roles"`
	IsActive     bool           `gorm:"default:true" json:"is_active"`
	IsVerified   bool           `gorm:"default:false" json:"is_verified"`
	LastLoginAt  *time.Time     `json:"last_login_at"`
	CreatedAt    time.Time      `json:"created_at"`
	UpdatedAt    time.Time      `json:"updated_at"`
	DeletedAt    gorm.DeletedAt `gorm:"index" json:"-"`
}

type Role struct {
	ID          uuid.UUID      `gorm:"type:uuid;primary_key" json:"id"`
	Name        string         `gorm:"uniqueIndex;not null" json:"name"`
	Description string         `json:"description"`
	Permissions []Permission   `gorm:"many2many:role_permissions;" json:"permissions"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
	DeletedAt   gorm.DeletedAt `gorm:"index" json:"-"`
}

type Permission struct {
	ID        uuid.UUID      `gorm:"type:uuid;primary_key" json:"id"`
	Resource  string         `gorm:"not null" json:"resource"`
	Action    string         `gorm:"not null" json:"action"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"-"`
}

// BeforeCreate hook to generate UUID
func (u *User) BeforeCreate(tx *gorm.DB) error {
	if u.ID == uuid.Nil {
		u.ID = uuid.New()
	}
	return nil
}

func (r *Role) BeforeCreate(tx *gorm.DB) error {
	if r.ID == uuid.Nil {
		r.ID = uuid.New()
	}
	return nil
}

func (p *Permission) BeforeCreate(tx *gorm.DB) error {
	if p.ID == uuid.Nil {
		p.ID = uuid.New()
	}
	return nil
}

// SetPassword hashes and sets the user password
func (u *User) SetPassword(password string) error {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	u.PasswordHash = string(hash)
	return nil
}

// CheckPassword verifies the password
func (u *User) CheckPassword(password string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(password))
	return err == nil
}

// GetRoleNames returns user's role names
func (u *User) GetRoleNames() []string {
	names := make([]string, len(u.Roles))
	for i, role := range u.Roles {
		names[i] = role.Name
	}
	return names
}

// HasRole checks if user has a specific role
func (u *User) HasRole(roleName string) bool {
	for _, role := range u.Roles {
		if role.Name == roleName {
			return true
		}
	}
	return false
}

// HasPermission checks if user has a specific permission
func (u *User) HasPermission(resource, action string) bool {
	for _, role := range u.Roles {
		for _, perm := range role.Permissions {
			if perm.Resource == resource && perm.Action == action {
				return true
			}
		}
	}
	return false
}