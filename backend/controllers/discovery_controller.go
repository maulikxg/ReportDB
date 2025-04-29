package controllers

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"v1/backend/database"
	"v1/backend/models"
	"v1/backend/utils"
)

// DiscoveryController handles HTTP requests for discovery profiles
type DiscoveryController struct {
	repo *database.DiscoveryRepository

	credRepo *database.CredentialRepository

	discoveryQueue *utils.DiscoveryQueue
}

// NewDiscoveryController creates a new discovery controller
func NewDiscoveryController(

	repo *database.DiscoveryRepository,

	credRepo *database.CredentialRepository,

	discoveryQueue *utils.DiscoveryQueue,

) *DiscoveryController {
	return &DiscoveryController{

		repo: repo,

		credRepo: credRepo,

		discoveryQueue: discoveryQueue,
	}

}

// Create handles creation of a new discovery profile
func (c *DiscoveryController) Create(ctx *gin.Context) {

	var input struct {
		CredentialIDs []int `json:"credential_ids" binding:"required"`

		IP string `json:"ip"`

		IPRange string `json:"ip_range"`
	}

	if err := ctx.ShouldBindJSON(&input); err != nil {

		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})

		return
	}

	// Validate that at least one IP or IP range is provided
	if input.IP == "" && input.IPRange == "" {

		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Either IP or IP range must be provided"})

		return
	}

	// Validate that credentials exist
	for _, credID := range input.CredentialIDs {

		_, err := c.credRepo.GetByID(credID)

		if err != nil {

			ctx.JSON(http.StatusBadRequest, gin.H{"error": "Credential ID " + strconv.Itoa(credID) + " not found"})

			return
		}
	}

	discovery := models.NewDiscoveryProfile(input.CredentialIDs, input.IP, input.IPRange)

	id, err := c.repo.Create(discovery)

	if err != nil {

		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})

		return
	}

	discovery.DiscoveryID = id

	// Queue the discovery for processing
	c.discoveryQueue.Add(discovery)

	ctx.JSON(http.StatusCreated, discovery)
}

// GetAll retrieves all discovery profiles
func (c *DiscoveryController) GetAll(ctx *gin.Context) {

	discoveries, err := c.repo.GetAll()

	if err != nil {

		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})

		return

	}

	ctx.JSON(http.StatusOK, discoveries)
}

// GetByID retrieves a discovery profile by ID
func (c *DiscoveryController) GetByID(ctx *gin.Context) {

	id, err := strconv.Atoi(ctx.Param("id"))

	if err != nil {

		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid ID format"})

		return

	}

	discovery, err := c.repo.GetByID(id)

	if err != nil {

		ctx.JSON(http.StatusNotFound, gin.H{"error": "Discovery profile not found"})

		return

	}

	ctx.JSON(http.StatusOK, discovery)
}

// Delete removes a discovery profile
func (c *DiscoveryController) Delete(ctx *gin.Context) {

	id, err := strconv.Atoi(ctx.Param("id"))

	if err != nil {

		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid ID format"})

		return

	}

	if err := c.repo.Delete(id); err != nil {

		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})

		return
		
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "Discovery profile deleted successfully"})
}
