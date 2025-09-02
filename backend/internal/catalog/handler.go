package catalog

import (
	"context"
	"deltacat/backend/internal/auth"
	"deltacat/backend/pkg/database"
	"deltacat/backend/pkg/models"
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type Service struct {
	db     *gorm.DB
	cache  *redis.Client
	logger *zap.Logger
}

// RegisterRoutes registers all catalog routes
func RegisterRoutes(api fiber.Router, db *gorm.DB, cache *redis.Client, logger *zap.Logger) {
	s := &Service{
		db:     db,
		cache:  cache,
		logger: logger,
	}

	catalog := api.Group("/catalog")

	// Catalog operations
	catalog.Get("/", s.ListCatalogs)
	catalog.Post("/", s.CreateCatalog)
	catalog.Get("/:catalogId", s.GetCatalog)
	catalog.Put("/:catalogId", s.UpdateCatalog)
	catalog.Delete("/:catalogId", s.DeleteCatalog)

	// Namespace operations
	catalog.Get("/:catalogId/namespaces", s.ListNamespaces)
	catalog.Post("/:catalogId/namespaces", s.CreateNamespace)
	catalog.Get("/:catalogId/namespaces/:namespaceId", s.GetNamespace)
	catalog.Put("/:catalogId/namespaces/:namespaceId", s.UpdateNamespace)
	catalog.Delete("/:catalogId/namespaces/:namespaceId", s.DeleteNamespace)

	// Table operations
	catalog.Get("/:catalogId/namespaces/:namespaceId/tables", s.ListTables)
	catalog.Post("/:catalogId/namespaces/:namespaceId/tables", s.CreateTable)
	catalog.Get("/:catalogId/namespaces/:namespaceId/tables/:tableId", s.GetTable)
	catalog.Put("/:catalogId/namespaces/:namespaceId/tables/:tableId", s.UpdateTable)
	catalog.Delete("/:catalogId/namespaces/:namespaceId/tables/:tableId", s.DeleteTable)

	// Table operations by name (Unity Catalog compatible)
	catalog.Get("/table/*", s.GetTableByPath)
	catalog.Post("/table/*", s.CreateTableByPath)
}

// ListCatalogs returns all catalogs
func (s *Service) ListCatalogs(c *fiber.Ctx) error {
	var catalogs []models.Catalog
	
	// Try cache first
	cacheKey := database.CacheKey("catalogs", "list")
	// Skip cache for now, implement later
	
	// Query database
	if err := s.db.Find(&catalogs).Error; err != nil {
		s.logger.Error("Failed to list catalogs", zap.Error(err))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to list catalogs",
		})
	}

	return c.JSON(catalogs)
}

// CreateCatalog creates a new catalog
func (s *Service) CreateCatalog(c *fiber.Ctx) error {
	var catalog models.Catalog
	if err := c.BodyParser(&catalog); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid request body",
		})
	}

	// Set owner to current user
	user := auth.GetUser(c)
	if user != nil {
		catalog.Owner = user.Email
	}

	// Create in database
	if err := s.db.Create(&catalog).Error; err != nil {
		s.logger.Error("Failed to create catalog", zap.Error(err))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to create catalog",
		})
	}

	// Invalidate cache
	s.cache.Del(context.Background(), database.CacheKey("catalogs", "list"))

	return c.Status(fiber.StatusCreated).JSON(catalog)
}

// GetCatalog returns a specific catalog
func (s *Service) GetCatalog(c *fiber.Ctx) error {
	catalogId := c.Params("catalogId")
	
	// Parse UUID
	id, err := uuid.Parse(catalogId)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid catalog ID",
		})
	}

	var catalog models.Catalog
	if err := s.db.Preload("Namespaces").First(&catalog, id).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "Catalog not found",
			})
		}
		s.logger.Error("Failed to get catalog", zap.Error(err))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to get catalog",
		})
	}

	return c.JSON(catalog)
}

// UpdateCatalog updates a catalog
func (s *Service) UpdateCatalog(c *fiber.Ctx) error {
	catalogId := c.Params("catalogId")
	
	id, err := uuid.Parse(catalogId)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid catalog ID",
		})
	}

	var updates models.Catalog
	if err := c.BodyParser(&updates); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid request body",
		})
	}

	// Update in database
	result := s.db.Model(&models.Catalog{}).Where("id = ?", id).Updates(updates)
	if result.Error != nil {
		s.logger.Error("Failed to update catalog", zap.Error(result.Error))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to update catalog",
		})
	}

	if result.RowsAffected == 0 {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": "Catalog not found",
		})
	}

	// Invalidate cache
	s.cache.Del(context.Background(), 
		database.CacheKey("catalogs", "list"),
		database.CacheKey("catalog", catalogId),
	)

	return c.JSON(fiber.Map{
		"message": "Catalog updated successfully",
	})
}

// DeleteCatalog deletes a catalog
func (s *Service) DeleteCatalog(c *fiber.Ctx) error {
	catalogId := c.Params("catalogId")
	
	id, err := uuid.Parse(catalogId)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid catalog ID",
		})
	}

	// Soft delete in database
	result := s.db.Delete(&models.Catalog{}, id)
	if result.Error != nil {
		s.logger.Error("Failed to delete catalog", zap.Error(result.Error))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to delete catalog",
		})
	}

	if result.RowsAffected == 0 {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": "Catalog not found",
		})
	}

	// Invalidate cache
	s.cache.Del(context.Background(), 
		database.CacheKey("catalogs", "list"),
		database.CacheKey("catalog", catalogId),
	)

	return c.JSON(fiber.Map{
		"message": "Catalog deleted successfully",
	})
}

// ListNamespaces returns all namespaces in a catalog
func (s *Service) ListNamespaces(c *fiber.Ctx) error {
	catalogId := c.Params("catalogId")
	
	id, err := uuid.Parse(catalogId)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid catalog ID",
		})
	}

	var namespaces []models.Namespace
	if err := s.db.Where("catalog_id = ?", id).Find(&namespaces).Error; err != nil {
		s.logger.Error("Failed to list namespaces", zap.Error(err))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to list namespaces",
		})
	}

	return c.JSON(namespaces)
}

// CreateNamespace creates a new namespace
func (s *Service) CreateNamespace(c *fiber.Ctx) error {
	catalogId := c.Params("catalogId")
	
	catalogUUID, err := uuid.Parse(catalogId)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid catalog ID",
		})
	}

	var namespace models.Namespace
	if err := c.BodyParser(&namespace); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid request body",
		})
	}

	namespace.CatalogID = catalogUUID
	
	// Set owner to current user
	user := auth.GetUser(c)
	if user != nil {
		namespace.Owner = user.Email
	}

	// Create in database
	if err := s.db.Create(&namespace).Error; err != nil {
		s.logger.Error("Failed to create namespace", zap.Error(err))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to create namespace",
		})
	}

	return c.Status(fiber.StatusCreated).JSON(namespace)
}

// GetNamespace returns a specific namespace
func (s *Service) GetNamespace(c *fiber.Ctx) error {
	namespaceId := c.Params("namespaceId")
	
	id, err := uuid.Parse(namespaceId)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid namespace ID",
		})
	}

	var namespace models.Namespace
	if err := s.db.Preload("Tables").First(&namespace, id).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "Namespace not found",
			})
		}
		s.logger.Error("Failed to get namespace", zap.Error(err))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to get namespace",
		})
	}

	return c.JSON(namespace)
}

// UpdateNamespace updates a namespace
func (s *Service) UpdateNamespace(c *fiber.Ctx) error {
	namespaceId := c.Params("namespaceId")
	
	id, err := uuid.Parse(namespaceId)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid namespace ID",
		})
	}

	var updates models.Namespace
	if err := c.BodyParser(&updates); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid request body",
		})
	}

	// Update in database
	result := s.db.Model(&models.Namespace{}).Where("id = ?", id).Updates(updates)
	if result.Error != nil {
		s.logger.Error("Failed to update namespace", zap.Error(result.Error))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to update namespace",
		})
	}

	if result.RowsAffected == 0 {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": "Namespace not found",
		})
	}

	return c.JSON(fiber.Map{
		"message": "Namespace updated successfully",
	})
}

// DeleteNamespace deletes a namespace
func (s *Service) DeleteNamespace(c *fiber.Ctx) error {
	namespaceId := c.Params("namespaceId")
	
	id, err := uuid.Parse(namespaceId)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid namespace ID",
		})
	}

	// Soft delete in database
	result := s.db.Delete(&models.Namespace{}, id)
	if result.Error != nil {
		s.logger.Error("Failed to delete namespace", zap.Error(result.Error))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to delete namespace",
		})
	}

	if result.RowsAffected == 0 {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": "Namespace not found",
		})
	}

	return c.JSON(fiber.Map{
		"message": "Namespace deleted successfully",
	})
}

// Table operations...
// ListTables returns all tables in a namespace
func (s *Service) ListTables(c *fiber.Ctx) error {
	namespaceId := c.Params("namespaceId")
	
	id, err := uuid.Parse(namespaceId)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid namespace ID",
		})
	}

	var tables []models.Table
	if err := s.db.Where("namespace_id = ?", id).Find(&tables).Error; err != nil {
		s.logger.Error("Failed to list tables", zap.Error(err))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to list tables",
		})
	}

	return c.JSON(tables)
}

// CreateTable creates a new table
func (s *Service) CreateTable(c *fiber.Ctx) error {
	namespaceId := c.Params("namespaceId")
	
	namespaceUUID, err := uuid.Parse(namespaceId)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid namespace ID",
		})
	}

	var table models.Table
	if err := c.BodyParser(&table); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid request body",
		})
	}

	table.NamespaceID = namespaceUUID
	
	// Set owner to current user
	user := auth.GetUser(c)
	if user != nil {
		table.Owner = user.Email
	}

	// Default format if not specified
	if table.Format == "" {
		table.Format = "delta"
	}

	// Create in database
	if err := s.db.Create(&table).Error; err != nil {
		s.logger.Error("Failed to create table", zap.Error(err))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to create table",
		})
	}

	// Invalidate cache
	cachePattern := fmt.Sprintf("tables:%s:*", namespaceId)
	database.InvalidatePattern(context.Background(), cachePattern)

	return c.Status(fiber.StatusCreated).JSON(table)
}

// GetTable returns a specific table
func (s *Service) GetTable(c *fiber.Ctx) error {
	tableId := c.Params("tableId")
	
	// Try cache first
	cacheKey := database.CacheKey("table", tableId)
	// Implement cache logic here
	
	id, err := uuid.Parse(tableId)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid table ID",
		})
	}

	var table models.Table
	if err := s.db.First(&table, id).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "Table not found",
			})
		}
		s.logger.Error("Failed to get table", zap.Error(err))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to get table",
		})
	}

	// Cache for 5 minutes
	database.SetWithTTL(context.Background(), cacheKey, table, 5*time.Minute)

	return c.JSON(table)
}

// UpdateTable updates a table
func (s *Service) UpdateTable(c *fiber.Ctx) error {
	tableId := c.Params("tableId")
	
	id, err := uuid.Parse(tableId)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid table ID",
		})
	}

	var updates models.Table
	if err := c.BodyParser(&updates); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid request body",
		})
	}

	// Update in database
	result := s.db.Model(&models.Table{}).Where("id = ?", id).Updates(updates)
	if result.Error != nil {
		s.logger.Error("Failed to update table", zap.Error(result.Error))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to update table",
		})
	}

	if result.RowsAffected == 0 {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": "Table not found",
		})
	}

	// Invalidate cache
	s.cache.Del(context.Background(), database.CacheKey("table", tableId))

	return c.JSON(fiber.Map{
		"message": "Table updated successfully",
	})
}

// DeleteTable deletes a table
func (s *Service) DeleteTable(c *fiber.Ctx) error {
	tableId := c.Params("tableId")
	
	id, err := uuid.Parse(tableId)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid table ID",
		})
	}

	// Soft delete in database
	result := s.db.Delete(&models.Table{}, id)
	if result.Error != nil {
		s.logger.Error("Failed to delete table", zap.Error(result.Error))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to delete table",
		})
	}

	if result.RowsAffected == 0 {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": "Table not found",
		})
	}

	// Invalidate cache
	s.cache.Del(context.Background(), database.CacheKey("table", tableId))

	return c.JSON(fiber.Map{
		"message": "Table deleted successfully",
	})
}

// GetTableByPath gets table by catalog.namespace.table path (Unity Catalog compatible)
func (s *Service) GetTableByPath(c *fiber.Ctx) error {
	path := c.Params("*")
	// Parse path: catalog.namespace.table
	// Implementation for Unity Catalog compatibility
	return c.JSON(fiber.Map{
		"message": "Get table by path",
		"path": path,
	})
}

// CreateTableByPath creates table by path (Unity Catalog compatible)
func (s *Service) CreateTableByPath(c *fiber.Ctx) error {
	path := c.Params("*")
	// Parse path and create table
	// Implementation for Unity Catalog compatibility
	return c.JSON(fiber.Map{
		"message": "Create table by path",
		"path": path,
	})
}