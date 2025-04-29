package controllers

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"v1/backend/database"
	"v1/backend/models"
	"v1/backend/poller"
	"v1/backend/utils"
)

type ProvisionController struct {
	repo            *database.ProvisionRepository
	credRepo        *database.CredentialRepository
	discRepo        *database.DiscoveryRepository
	provisionNotify *utils.ProvisionNotifier
	pollerConfig    *poller.Configurer
}

// NewProvisionController creates a new provision controller
func NewProvisionController(
	repo *database.ProvisionRepository,
	credRepo *database.CredentialRepository,
	discRepo *database.DiscoveryRepository,
	provisionNotify *utils.ProvisionNotifier,
	pollerConfig *poller.Configurer,
) *ProvisionController {
	return &ProvisionController{
		repo:            repo,
		credRepo:        credRepo,
		discRepo:        discRepo,
		provisionNotify: provisionNotify,
		pollerConfig:    pollerConfig,
	}
}

// Create handles creation of a new provision
func (c *ProvisionController) Create(ctx *gin.Context) {
	var input struct {
		IP           string `json:"ip" binding:"required"`
		CredentialID int    `json:"credential_id" binding:"required"`
		DiscoveryID  int    `json:"discovery_id" binding:"required"`
	}

	if err := ctx.ShouldBindJSON(&input); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Validate credential exists
	credential, err := c.credRepo.GetByID(input.CredentialID)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Credential not found"})
		return
	}

	// Validate discovery exists
	_, err = c.discRepo.GetByID(input.DiscoveryID)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Discovery not found"})
		return
	}

	provision := models.NewProvision(input.IP, input.CredentialID, input.DiscoveryID)
	id, err := c.repo.Create(provision)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	provision.ObjectID = id

	// Notify the poller system about the new provision
	if err := c.provisionNotify.Notify(provision); err != nil {
		// Just log the error, but continue
		ctx.JSON(http.StatusCreated, gin.H{
			"provision": provision,
			"warning":   "Failed to notify poller: " + err.Error(),
		})
		return
	}

	// Configure the poller to start polling this device
	if err := c.pollerConfig.AddDevice(provision, credential); err != nil {
		// Just log the error, but continue
		ctx.JSON(http.StatusCreated, gin.H{
			"provision": provision,
			"warning":   "Failed to configure poller: " + err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusCreated, provision)
}

// GetAll retrieves all provisions
func (c *ProvisionController) GetAll(ctx *gin.Context) {
	provisions, err := c.repo.GetAll()
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, provisions)
}

// GetByID retrieves a provision by ID
func (c *ProvisionController) GetByID(ctx *gin.Context) {
	id, err := strconv.Atoi(ctx.Param("id"))
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid ID format"})
		return
	}

	provision, err := c.repo.GetByID(id)
	if err != nil {
		ctx.JSON(http.StatusNotFound, gin.H{"error": "Provision not found"})
		return
	}

	ctx.JSON(http.StatusOK, provision)
}

// GetByDiscoveryID retrieves provisions by discovery ID
func (c *ProvisionController) GetByDiscoveryID(ctx *gin.Context) {
	id, err := strconv.Atoi(ctx.Param("discovery_id"))
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid ID format"})
		return
	}

	provisions, err := c.repo.GetByDiscoveryID(id)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, provisions)
}

// UpdateStatus updates the provision status
func (c *ProvisionController) UpdateStatus(ctx *gin.Context) {
	id, err := strconv.Atoi(ctx.Param("id"))
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid ID format"})
		return
	}

	var input struct {
		IsProvisioned bool `json:"is_provisioned" binding:"required"`
	}

	if err := ctx.ShouldBindJSON(&input); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	provision, err := c.repo.GetByID(id)
	if err != nil {
		ctx.JSON(http.StatusNotFound, gin.H{"error": "Provision not found"})
		return
	}

	if err := c.repo.UpdateProvisionStatus(id, input.IsProvisioned); err != nil {

		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})

		return

	}

	// If provision is enabled, configure the poller
	if input.IsProvisioned {

		credential, err := c.credRepo.GetByID(provision.CredentialID)

		if err != nil {

			ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get credential details"})

			return

		}

		// Configure the poller to start polling this device
		if err := c.pollerConfig.AddDevice(provision, credential); err != nil {

			// Just log the error, but continue

			ctx.JSON(http.StatusOK, gin.H{

				"message": "Provision status updated successfully",

				"warning": "Failed to configure poller: " + err.Error(),
			})

			return

		}

	} else {

		// If provision is disabled, remove from poller configuration
		if err := c.pollerConfig.RemoveDevice(provision.ObjectID); err != nil {

			// Just log the error, but continue
			ctx.JSON(http.StatusOK, gin.H{

				"message": "Provision status updated successfully",

				"warning": "Failed to remove device from poller: " + err.Error(),
			})

			return
		}
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "Provision status updated successfully"})
}

// Delete removes a provision
func (c *ProvisionController) Delete(ctx *gin.Context) {

	id, err := strconv.Atoi(ctx.Param("id"))

	if err != nil {

		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid ID format"})

		return

	}

	provision, err := c.repo.GetByID(id)

	if err != nil {

		ctx.JSON(http.StatusNotFound, gin.H{"error": "Provision not found"})

		return

	}

	// Remove from poller configuration first
	if provision.IsProvisioned {

		if err := c.pollerConfig.RemoveDevice(provision.ObjectID); err != nil {

			// Just log the error, but continue
			ctx.JSON(http.StatusOK, gin.H{

				"warning": "Failed to remove device from poller: " + err.Error(),
			})

			// Continue with deletion
		}
	}

	if err := c.repo.Delete(id); err != nil {

		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})

		return

	}

	ctx.JSON(http.StatusOK, gin.H{"message": "Provision deleted successfully"})
}
