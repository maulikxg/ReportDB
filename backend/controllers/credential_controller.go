package controllers

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"v1/backend/database"
	"v1/backend/models"
)

// CredentialController handles HTTP requests for credential profiles
type CredentialController struct {
	repo *database.CredentialRepository
}

func NewCredentialController(repo *database.CredentialRepository) *CredentialController {

	return &CredentialController{repo: repo}

}

func (c *CredentialController) Create(ctx *gin.Context) {

	var input struct {
		Username string `json:"username" binding:"required"`

		Password string `json:"password" binding:"required"`

		Port int `json:"port" binding:"required"`
	}

	if err := ctx.ShouldBindJSON(&input); err != nil {

		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})

		return

	}

	credential := models.NewCredentialProfile(input.Username, input.Password, input.Port)

	id, err := c.repo.Create(credential)

	if err != nil {

		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})

		return

	}

	credential.CredentialID = id

	ctx.JSON(http.StatusCreated, credential)
}

// GetAll retrieves all credential profiles
func (c *CredentialController) GetAll(ctx *gin.Context) {

	credentials, err := c.repo.GetAll()

	if err != nil {

		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})

		return

	}

	ctx.JSON(http.StatusOK, credentials)
}

// GetByID retrieves a credential profile by ID
func (c *CredentialController) GetByID(ctx *gin.Context) {

	id, err := strconv.Atoi(ctx.Param("id"))

	if err != nil {

		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid ID format"})

		return

	}

	credential, err := c.repo.GetByID(id)

	if err != nil {

		ctx.JSON(http.StatusNotFound, gin.H{"error": "Credential profile not found"})

		return

	}

	ctx.JSON(http.StatusOK, credential)

}

// Update updates a credential profile
func (c *CredentialController) Update(ctx *gin.Context) {

	id, err := strconv.Atoi(ctx.Param("id"))

	if err != nil {

		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid ID format"})

		return

	}

	credential, err := c.repo.GetByID(id)

	if err != nil {

		ctx.JSON(http.StatusNotFound, gin.H{"error": "Credential profile not found"})

		return

	}

	var input struct {
		Username string `json:"username"`

		Password string `json:"password"`

		Port int `json:"port"`
	}

	if err := ctx.ShouldBindJSON(&input); err != nil {

		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})

		return

	}

	if input.Username != "" {

		credential.Username = input.Username

	}
	if input.Password != "" {

		credential.Password = input.Password

	}
	if input.Port != 0 {

		credential.Port = input.Port

	}

	credential.UpdatedAt = time.Now()

	if err := c.repo.Update(credential); err != nil {

		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})

		return

	}

	ctx.JSON(http.StatusOK, credential)

}

// Delete removes a credential profile
func (c *CredentialController) Delete(ctx *gin.Context) {

	id, err := strconv.Atoi(ctx.Param("id"))

	if err != nil {

		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid ID format"})

		return

	}

	if err := c.repo.Delete(id); err != nil {

		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})

		return

	}

	ctx.JSON(http.StatusOK, gin.H{"message": "Credential profile deleted successfully"})
}
