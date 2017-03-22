//
//  RedisClientPoolTests.swift
//  RedisClient
//
//  Created by Valerio Mazzeo on 08/03/2017.
//  Copyright © 2017 VMLabs Limited. All rights reserved.
//
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//  See the GNU Lesser General Public License for more details.
//
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program. If not, see <http://www.gnu.org/licenses/>.
//

import XCTest
import Foundation
import Dispatch
@testable import RedisClient

class RedisClientPoolTests: XCTestCase {

    static let allTests = [
        ("testExecute", testExecute),
        ("testMulti", testMulti)
        ]

    func testExecute() throws {

        let pool = RedisClientPool(maxElementCount: 2) { MockClient() }

        var response: RedisClientResponse?
        var anotherResponse: RedisClientResponse?

        let responseExpectation = self.expectation(description: "responseExpectation")

        DispatchQueue(label: "com.reswifq.RedisClientPoolTests").async {
            response = try? pool.execute("TEST")
            responseExpectation.fulfill()
        }

        let anotherResponseExpectation = self.expectation(description: "anotherResponseExpectation")

        DispatchQueue(label: "com.reswifq.RedisClientPoolTests").async {
            anotherResponse = try? pool.execute("TEST")
            anotherResponseExpectation.fulfill()
        }

        self.waitForExpectations(timeout: 4.0, handler: nil)

        XCTAssertNotNil(response)
        XCTAssertNotNil(anotherResponse)
        XCTAssertNotEqual(response?.string, anotherResponse?.string)
    }

    func testMulti() throws {

        let pool = RedisClientPool(maxElementCount: 2) { MockClient() }

        var clientID: String?
        var anotherClientID: String?

        let clientIDExpectation = self.expectation(description: "clientIDExpectation")

        DispatchQueue(label: "com.reswifq.RedisClientPoolTests").async {

            _ = try? pool.multi { client, transaction in
                clientID = (client as! MockClient).identifier
                clientIDExpectation.fulfill()
            }
        }

        let anotherClientIDExpectation = self.expectation(description: "anotherClientIDExpectation")

        DispatchQueue(label: "com.reswifq.RedisClientPoolTests").async {

            _ = try? pool.multi { client, transaction in
                anotherClientID = (client as! MockClient).identifier
                anotherClientIDExpectation.fulfill()
            }
        }

        self.waitForExpectations(timeout: 4.0, handler: nil)

        XCTAssertNotNil(clientID)
        XCTAssertNotNil(anotherClientID)
        XCTAssertNotEqual(clientID, anotherClientID)
    }
}

extension RedisClientPoolTests {

    class MockClient: RedisClient {

        let identifier = UUID().uuidString

        func execute(_ command: String, arguments: [String]?) throws -> RedisClientResponse {

            sleep(1)

            switch command {
            case "MULTI":
                return RedisClientResponse.status(.ok)
            default:
                return RedisClientResponse.string(self.identifier)
            }
        }
    }
}
