env "local" {
  url = getenv("DATABASE_URL")
  migration {
    dir = "file://migrations"
  }
}

env "production" {
  url = getenv("DATABASE_URL")
  migration {
    dir = "file://migrations"
  }
}
