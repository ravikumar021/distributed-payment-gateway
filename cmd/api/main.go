package main

import (
    "github.com/gin-gonic/gin"
)

func main() {
    r := gin.Default()

    r.POST("/payment", func(c *gin.Context) {
        c.JSON(202, gin.H{"status": "processing"})
    })

    r.Run(":8080")
}
