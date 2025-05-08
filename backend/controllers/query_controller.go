package controllers

import (
	"github.com/gin-gonic/gin"
	"net/http"
	"v1/backend/reportdb"
)

type QueryController struct {
	reportClient *reportdb.Client
}

func NewQueryController(reportClient *reportdb.Client) *QueryController {
	return &QueryController{
		reportClient: reportClient,
	}
}

// RouteQuery handles query routing from client to ReportDB
func (c *QueryController) RouteQuery(ctx *gin.Context) {
	var query reportdb.Query
	if err := ctx.ShouldBindJSON(&query); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid query format"})
		return
	}

	// Forward query to ReportDB
	response, err := c.reportClient.SendQuery(query)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, response)
} 