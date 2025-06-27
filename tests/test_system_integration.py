#!/usr/bin/env python3
"""
System Integration Test Suite - Dan's Comprehensive Service Validator

This script systematically tests each service in our book recommendation 
architecture to identify what's working, what's broken, and what needs fixing.

Why this matters: We need to know exactly what's operational before the 
presentation. No surprises, no "it worked on my machine" bullshit.
"""
import asyncio
import requests
import asyncpg
import json
from pathlib import Path

# Test configuration
SERVICES = {
    "recommendation_api": {"url": "http://localhost:8000", "critical": True},
    "streamlit_ui": {"url": "http://localhost:8501", "critical": True},
    "prometheus": {"url": "http://localhost:9090", "critical": False},
    "postgres": {"host": "localhost", "port": 5432, "critical": True},
    "redis": {"host": "localhost", "port": 6379, "critical": False},
}

class SystemTester:
    def __init__(self):
        self.results = {}
        self.critical_failures = []
    
    def log_result(self, service: str, test: str, status: str, details: str = ""):
        """Log test results in a structured way"""
        if service not in self.results:
            self.results[service] = {}
        
        self.results[service][test] = {
            "status": status,
            "details": details
        }
        
        # Track critical failures
        if status == "FAIL" and SERVICES.get(service, {}).get("critical", False):
            self.critical_failures.append(f"{service}: {test}")
        
        status_emoji = {"PASS": "✅", "FAIL": "❌", "WARN": "⚠️"}.get(status, "❓")
        print(f"  {status_emoji} {test}: {details}")
    
    async def test_recommendation_api(self):
        """Test the core recommendation API - this MUST work"""
        print("\n🎯 TESTING RECOMMENDATION API")
        service = "recommendation_api"
        
        # Health check
        try:
            response = requests.get(f"{SERVICES[service]['url']}/health", timeout=5)
            if response.status_code == 200:
                self.log_result(service, "health_check", "PASS", f"Status: {response.status_code}")
            else:
                self.log_result(service, "health_check", "FAIL", f"Status: {response.status_code}")
        except Exception as e:
            self.log_result(service, "health_check", "FAIL", str(e))
        
        # Recommendation endpoint
        try:
            response = requests.post(
                f"{SERVICES[service]['url']}/recommend",
                params={"student_id": "S001", "query": "adventure books", "n": 2},
                timeout=30
            )
            if response.status_code == 200:
                data = response.json()
                rec_count = len(data.get("recommendations", []))
                self.log_result(service, "recommendation_endpoint", "PASS", 
                               f"Returned {rec_count} recommendations")
            else:
                self.log_result(service, "recommendation_endpoint", "FAIL", 
                               f"Status: {response.status_code}")
        except Exception as e:
            self.log_result(service, "recommendation_endpoint", "FAIL", str(e))
    
    async def test_streamlit_ui(self):
        """Test the Streamlit UI - teachers need this to work"""
        print("\n📊 TESTING STREAMLIT UI")
        service = "streamlit_ui"
        
        try:
            response = requests.get(SERVICES[service]["url"], timeout=10)
            if response.status_code == 200 and "streamlit" in response.text.lower():
                self.log_result(service, "ui_accessible", "PASS", "UI loads correctly")
            else:
                self.log_result(service, "ui_accessible", "FAIL", f"Status: {response.status_code}")
        except Exception as e:
            self.log_result(service, "ui_accessible", "FAIL", str(e))
    
    async def test_database(self):
        """Test PostgreSQL database - core data storage"""
        print("\n🗄️  TESTING DATABASE")
        service = "postgres"
        
        try:
            conn = await asyncpg.connect(
                host=SERVICES[service]["host"],
                port=SERVICES[service]["port"],
                user="books",
                password="books",
                database="books"
            )
            
            # Test basic connectivity
            self.log_result(service, "connectivity", "PASS", "Connected successfully")
            
            # Test critical tables exist
            tables = await conn.fetch("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            table_names = [t["table_name"] for t in tables]
            
            critical_tables = ["catalog", "students", "checkout"]
            missing_tables = [t for t in critical_tables if t not in table_names]
            
            if not missing_tables:
                self.log_result(service, "schema", "PASS", f"All critical tables exist: {critical_tables}")
            else:
                self.log_result(service, "schema", "FAIL", f"Missing tables: {missing_tables}")
            
            # Test data exists
            catalog_count = await conn.fetchval("SELECT COUNT(*) FROM catalog")
            student_count = await conn.fetchval("SELECT COUNT(*) FROM students")
            checkout_count = await conn.fetchval("SELECT COUNT(*) FROM checkout")
            
            if catalog_count > 0 and student_count > 0 and checkout_count > 0:
                self.log_result(service, "data", "PASS", 
                               f"Data loaded: {catalog_count} books, {student_count} students, {checkout_count} checkouts")
            else:
                self.log_result(service, "data", "WARN", 
                               f"Low data counts: {catalog_count} books, {student_count} students, {checkout_count} checkouts")
            
            await conn.close()
            
        except Exception as e:
            self.log_result(service, "connectivity", "FAIL", str(e))
    
    async def test_workers(self):
        """Test background workers - these should be running but failures aren't critical"""
        print("\n⚙️  TESTING BACKGROUND WORKERS")
        
        # We can't easily test workers directly, but we can check their logs
        # and see if they're processing data
        
        # For now, just check if they're running (would need docker API for full test)
        self.log_result("workers", "status_check", "WARN", "Manual log review required")
    
    def generate_summary(self):
        """Generate a comprehensive summary for Dan"""
        print("\n" + "="*60)
        print("🎯 SYSTEM STATUS SUMMARY")
        print("="*60)
        
        total_tests = sum(len(tests) for tests in self.results.values())
        passed_tests = sum(1 for tests in self.results.values() 
                          for result in tests.values() if result["status"] == "PASS")
        
        print(f"📊 Overall: {passed_tests}/{total_tests} tests passed")
        
        if self.critical_failures:
            print(f"\n❌ CRITICAL FAILURES ({len(self.critical_failures)}):")
            for failure in self.critical_failures:
                print(f"   • {failure}")
            print("\n🚨 THESE MUST BE FIXED BEFORE PRESENTATION!")
        else:
            print("\n✅ NO CRITICAL FAILURES - SYSTEM IS PRESENTATION READY!")
        
        print(f"\n📋 DETAILED RESULTS:")
        for service, tests in self.results.items():
            print(f"\n{service.upper()}:")
            for test, result in tests.items():
                status_emoji = {"PASS": "✅", "FAIL": "❌", "WARN": "⚠️"}.get(result["status"], "❓")
                print(f"   {status_emoji} {test}: {result['details']}")

async def main():
    """Run the full system test suite"""
    print("🔧 STARTING COMPREHENSIVE SYSTEM TEST")
    print("Time to find out what's actually working...")
    
    tester = SystemTester()
    
    # Run all tests
    await tester.test_recommendation_api()
    await tester.test_streamlit_ui() 
    await tester.test_database()
    await tester.test_workers()
    
    # Generate summary
    tester.generate_summary()
    
    # Return exit code based on critical failures
    return len(tester.critical_failures)

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code) 