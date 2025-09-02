package middleware

import (
	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
)

// CustomErrorHandler handles all errors in the application
func CustomErrorHandler(c *fiber.Ctx, err error) error {
	// Get logger from context if available
	logger, _ := zap.NewProduction()
	sugar := logger.Sugar()

	// Default to 500
	code := fiber.StatusInternalServerError
	message := "Internal Server Error"

	// Check if it's a Fiber error
	if e, ok := err.(*fiber.Error); ok {
		code = e.Code
		message = e.Message
	}

	// Log the error
	sugar.Errorw("Request error",
		"error", err,
		"status", code,
		"path", c.Path(),
		"method", c.Method(),
		"ip", c.IP(),
		"request_id", c.Locals("requestid"),
	)

	// Return JSON error response
	return c.Status(code).JSON(fiber.Map{
		"error": message,
		"request_id": c.Locals("requestid"),
	})
}